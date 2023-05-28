//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/as_bytes.hpp>
#include <vast/chunk.hpp>
#include <vast/concept/parseable/string/quoted_string.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/si_literals.hpp>

#include <boost/asio.hpp>
#include <boost/process.hpp>
#include <caf/detail/scope_guard.hpp>

#include <mutex>
#include <queue>
#include <thread>

namespace bp = boost::process;

namespace vast::plugins::shell {
namespace {

using namespace vast::binary_byte_literals;

/// The block size when reading from the child's stdin.
constexpr auto block_size = 16_KiB;

/// Wraps the logic for interacting with a child's stdin and stdout.
class child {
public:
  static auto make(std::string command) -> caf::expected<child> {
    auto result = child{std::move(command)};
    try {
      auto exit_handler = [](int exit, std::error_code ec) {
        VAST_DEBUG("child exited with code {}: {}", exit, ec.message());
      };
      result.child_ = bp::child{
        result.command_,
        bp::std_out > result.stdout_,
        bp::std_in < result.stdin_,
        bp::on_exit(exit_handler),
      };
    } catch (const bp::process_error& e) {
      return caf::make_error(ec::filesystem_error, e.what());
    }
    return result;
  }

  auto reading() -> bool {
    return child_.running() && not stdout_.eof();
  }

  auto writing() -> bool {
    return child_.running() && not stdin_.eof();
  }

  auto read(std::span<std::byte> buffer) -> caf::expected<size_t> {
    VAST_ASSERT(!buffer.empty());
    VAST_DEBUG("trying to read {} bytes", buffer.size());
    auto* data = reinterpret_cast<char*>(buffer.data());
    auto size = detail::narrow_cast<std::streamsize>(buffer.size());
    stdout_.read(data, size);
    auto bytes_read = stdout_.gcount();
    VAST_DEBUG("read {} bytes", bytes_read);
    return detail::narrow_cast<size_t>(bytes_read);
  }

  auto write(std::span<const std::byte> buffer) -> caf::error {
    VAST_ASSERT(!buffer.empty());
    VAST_DEBUG("writing {} bytes to child's stdin", buffer.size());
    const auto* data = reinterpret_cast<const char*>(buffer.data());
    auto size = detail::narrow_cast<std::streamsize>(buffer.size());
    if (not stdin_.write(data, size))
      return caf::make_error(ec::unspecified,
                             "failed to write into child's stdin");
    return caf::none;
  }

  void close_stdin() {
    // See https://github.com/boostorg/process/issues/125 for why we
    // seemingly double-close the pipe.
    VAST_DEBUG("sending EOF to child's stdin");
    stdin_.close();
    stdin_.pipe().close();
  }

private:
  explicit child(std::string command) : command_{std::move(command)} {
    VAST_ASSERT(!command_.empty());
  }

  std::string command_;
  bp::child child_;
  bp::ipstream stdout_;
  bp::opstream stdin_;
};

class shell_operator final : public crtp_operator<shell_operator> {
public:
  explicit shell_operator(std::string command) : command_{std::move(command)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto child = child::make(command_);
    if (!child) {
      ctrl.abort(child.error());
      co_return;
    }
    while (child->reading()) {
      std::vector<char> buffer(block_size);
      if (auto bytes_read = child->read(as_writeable_bytes(buffer))) {
        if (bytes_read == 0) {
          co_yield {};
          continue;
        }
        buffer.resize(*bytes_read);
        auto chk = chunk::make(std::exchange(buffer, {}));
        VAST_DEBUG("yielding chunk with {} bytes", chk->size());
        co_yield chk;
      }
    }
  }

  //auto
  //operator()(generator<chunk_ptr> input, operator_control_plane& ctrl) const
  //  -> generator<std::monostate> {
  //  auto child = child::make(command_);
  //  if (!child) {
  //    ctrl.abort(child.error());
  //    co_return;
  //  }
  //  auto at_exit = caf::detail::make_scope_guard([&] {
  //    child->close_stdin();
  //  });
  //  // Loop over input chunks.
  //  for (auto&& chunk : input) {
  //    if (not chunk || chunk->size() == 0 || not child->writing()) {
  //      co_yield {};
  //      continue;
  //    }
  //    // Pass operator input to the child's stdin.
  //    auto err = child->write(as_bytes(*chunk));
  //    co_yield {};
  //    if (err) {
  //      ctrl.abort(err);
  //      break;
  //    }
  //  }
  //}

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto child = child::make(command_);
    if (!child) {
      ctrl.abort(child.error());
      co_return;
    }
    // Read from child in separate thread because coroutine-based async I/O is
    // not (yet) feasible. The thread writes the chunks into a queue such that
    // to this coroutine can yield them.
    std::queue<chunk_ptr> chunks;
    std::mutex chunks_mutex;
    auto thread = std::thread([&child, &chunks, &chunks_mutex] {
      while (child->reading()) {
        std::vector<char> buffer(block_size);
        if (auto bytes_read = child->read(as_writeable_bytes(buffer))) {
          if (*bytes_read > 0) {
            buffer.resize(*bytes_read);
            auto chk = chunk::make(std::exchange(buffer, {}));
            std::lock_guard lock{chunks_mutex};
            chunks.push(std::move(chk));
          }
        }
      }
    });
    {
      // Coroutines require RAII-style exit handling.
      auto at_exit = caf::detail::make_scope_guard([&] {
        child->close_stdin();
        VAST_DEBUG("joining thread");
        thread.join();
      });
      // Loop over input chunks.
      for (auto&& chunk : input) {
        if (not chunk || chunk->size() == 0 || not child->writing()) {
          co_yield {};
          continue;
        }
        // Pass operator input to the child's stdin.
        if (auto err = child->write(as_bytes(*chunk))) {
          ctrl.abort(err);
          co_yield {};
          break;
        }
        // Try yielding so far accumulated child output.
        std::unique_lock lock{chunks_mutex, std::try_to_lock};
        if (lock.owns_lock()) {
          size_t i = 0;
          auto total = chunks.size();
          while (not chunks.empty()) {
            auto chk = chunks.front();
            chunks.pop();
            VAST_DEBUG("yielding chunk {}/{} with {} bytes", ++i, total,
                       chk->size());
            co_yield chk;
          }
        } else {
          co_yield {};
        }
      }
    }
    // Yield all accumulated child output.
    std::lock_guard lock{chunks_mutex};
    size_t i = 0;
    auto total = chunks.size();
    while (not chunks.empty()) {
      auto chk = chunks.front();
      VAST_DEBUG("yielding chunk {}/{} with {} bytes", ++i, total, chk->size());
      co_yield chk;
      chunks.pop();
    }
  }

  auto to_string() const -> std::string override {
    return fmt::format("shell \"{}\"", command_);
  }

  auto location() const -> operator_location override {
    // The user expectation is that shell executes relative to the currently
    // executing process.
    return operator_location::local;
  }

  auto detached() const -> bool override {
    // We may execute blocking syscalls.
    return true;
  }

private:
  std::string command_;
};

class plugin final : public virtual operator_plugin {
public:
  // plugin API
  auto initialize([[maybe_unused]] const record& plugin_config,
                  [[maybe_unused]] const record& global_config)
    -> caf::error override {
    return {};
  }

  [[nodiscard]] auto name() const -> std::string override {
    return "shell";
  };

  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::operator_arg;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = required_ws_or_comment >> operator_arg
                   >> optional_ws_or_comment >> end_of_pipeline_operator;
    std::string command;
    if (not p(f, l, command)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error,
                        fmt::format("failed to parse {} operator: '{}'", name(),
                                    pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<shell_operator>(std::move(command)),
    };
  }
};

} // namespace
} // namespace vast::plugins::shell

VAST_REGISTER_PLUGIN(vast::plugins::shell::plugin)
