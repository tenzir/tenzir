//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

namespace vast::plugins::shell {
namespace {

class shell_operator final : public crtp_operator<shell_operator> {
public:
  explicit shell_operator(std::string command) : command_{std::move(command)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    using namespace binary_byte_literals;
    namespace bp = boost::process;
    bp::ipstream child_stdout;
    bp::child child;
    try {
      auto exit_handler = [this](int exit, std::error_code ec) {
        VAST_DEBUG("child \"{}\" exited with code {}: {}", command_, exit,
                   ec.message());
      };
      child = bp::child{
        command_,
        bp::std_out > child_stdout,
        bp::on_exit(exit_handler),
      };
    } catch (const bp::process_error& e) {
      ctrl.abort(caf::make_error(ec::filesystem_error, e.what()));
      co_return;
    }
    constexpr auto block_size = 16_KiB;
    while (child.running() && not child_stdout.eof()) {
      // Read from child in a blocking manner. This works because we're
      // in our own thread.
      VAST_DEBUG("trying to read {} bytes", block_size);
      std::vector<char> buffer(block_size);
      child_stdout.read(buffer.data(), block_size);
      auto bytes_read = child_stdout.gcount();
      VAST_DEBUG("read {} bytes", bytes_read);
      if (bytes_read == 0) {
        co_yield {};
        continue;
      }
      buffer.resize(bytes_read);
      auto chk = chunk::make(std::exchange(buffer, {}));
      VAST_DEBUG("yielding chunk with {} bytes", bytes_read);
      co_yield chk;
    }
  }

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    using namespace binary_byte_literals;
    namespace bp = boost::process;
    // Spawn child process and connect stdin and stdout.
    bp::ipstream child_stdout;
    bp::opstream child_stdin;
    bp::child child;
    try {
      auto exit_handler = [this](int exit, std::error_code ec) {
        VAST_DEBUG("child \"{}\" exited with code {}: {}", command_, exit,
                   ec.message());
      };
      child = bp::child{
        command_,
        bp::std_out > child_stdout,
        bp::std_in < child_stdin,
        bp::on_exit(exit_handler),
      };
    } catch (const bp::process_error& e) {
      ctrl.abort(caf::make_error(ec::filesystem_error, e.what()));
      co_return;
    }
    // Read from child in separate thread because co-routine based async I/O is
    // not yet feasible. The thread hands over protected chunks.
    std::queue<chunk_ptr> chunks;
    std::mutex chunks_mutex;
    auto thread = std::thread([&child_stdout, &chunks, &chunks_mutex] {
      constexpr auto block_size = 16_KiB;
      while (not child_stdout.eof()) {
        VAST_DEBUG("trying to read {} bytes", block_size);
        std::vector<char> buffer(block_size);
        child_stdout.read(buffer.data(), block_size);
        auto bytes_read = child_stdout.gcount();
        VAST_DEBUG("read {} bytes", bytes_read);
        if (bytes_read > 0) {
          buffer.resize(bytes_read);
          auto chk = chunk::make(std::exchange(buffer, {}));
          std::lock_guard lock{chunks_mutex};
          chunks.push(std::move(chk));
        }
      }
    });
    {
      // Coroutines require RAII-style exit handling.
      auto at_exit = caf::detail::make_scope_guard([&] {
        // See https://github.com/boostorg/process/issues/125 for why we
        // seemingly double-close the pipe.
        VAST_DEBUG("sending EOF to child's stdin");
        child_stdin.close();
        child_stdin.pipe().close();
        VAST_DEBUG("joining thread");
        thread.join();
      });
      // Loop over input chunks.
      for (auto&& chunk : input) {
        if (not chunk || chunk->size() == 0 || not child.running()) {
          co_yield {};
          continue;
        }
        // Pass operator input to the child's stdin.
        const auto* chunk_data = reinterpret_cast<const char*>(chunk->data());
        auto chunk_size = detail::narrow_cast<std::streamsize>(chunk->size());
        VAST_DEBUG("writing {} bytes to child's stdin", chunk_size);
        if (not child_stdin.write(chunk_data, chunk_size)) {
          ctrl.abort(
            caf::make_error(ec::unspecified,
                            fmt::format("failed to write into child's stdin")));
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
