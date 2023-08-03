//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/string/quoted_string.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>

#include <boost/asio.hpp>
#include <boost/process.hpp>
#include <caf/detail/scope_guard.hpp>

#include <mutex>
#include <queue>
#include <thread>

namespace bp = boost::process;

namespace tenzir::plugins::shell {
namespace {

using namespace tenzir::binary_byte_literals;

/// The block size when reading from the child's stdin.
constexpr auto block_size = 16_KiB;

enum class stdin_mode { none, inherit, pipe };

/// Wraps the logic for interacting with a child's stdin and stdout.
class child {
public:
  static auto make(std::string command, stdin_mode mode)
    -> caf::expected<child> {
    auto result = child{std::move(command)};
    try {
      auto exit_handler = [](int exit, std::error_code ec) {
        TENZIR_DEBUG("child exited with code {}: {}", exit, ec.message());
      };
      switch (mode) {
        case stdin_mode::none:
          result.child_ = bp::child{
            result.command_,
            bp::std_out > result.stdout_,
            bp::std_in < bp::close,
            bp::on_exit(exit_handler),
          };
          break;
        case stdin_mode::inherit:
          result.child_ = bp::child{
            result.command_,
            bp::std_out > result.stdout_,
            bp::on_exit(exit_handler),
          };
          break;
        case stdin_mode::pipe:
          result.child_ = bp::child{
            result.command_,
            bp::std_out > result.stdout_,
            bp::std_in < result.stdin_,
            bp::on_exit(exit_handler),
          };
          break;
      }
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
    TENZIR_ASSERT(!buffer.empty());
    TENZIR_DEBUG("trying to read {} bytes", buffer.size());
    auto* data = reinterpret_cast<char*>(buffer.data());
    auto size = detail::narrow_cast<std::streamsize>(buffer.size());
    stdout_.read(data, size);
    auto bytes_read = stdout_.gcount();
    TENZIR_DEBUG("read {} bytes", bytes_read);
    return detail::narrow_cast<size_t>(bytes_read);
  }

  auto write(std::span<const std::byte> buffer) -> caf::error {
    TENZIR_ASSERT(!buffer.empty());
    TENZIR_DEBUG("writing {} bytes to child's stdin", buffer.size());
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
    TENZIR_DEBUG("sending EOF to child's stdin");
    stdin_.close();
    stdin_.pipe().close();
  }

  auto wait() -> caf::error {
    auto ec = std::error_code{};
    child_.wait(ec);
    if (ec) {
      return caf::make_error(ec::unspecified,
                             fmt::format("waiting for child process failed: {}",
                                         ec));
    }
    auto code = child_.exit_code();
    if (code != 0) {
      return caf::make_error(
        ec::unspecified,
        fmt::format("child process exited with exit-code {}", code));
    }
    return {};
  }

private:
  explicit child(std::string command) : command_{std::move(command)} {
    TENZIR_ASSERT(!command_.empty());
  }

  std::string command_;
  bp::child child_;
  bp::ipstream stdout_;
  bp::opstream stdin_;
};

class shell_operator final : public crtp_operator<shell_operator> {
public:
  shell_operator() = default;

  explicit shell_operator(std::string command) : command_{std::move(command)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto mode = ctrl.has_terminal() ? stdin_mode::inherit : stdin_mode::none;
    auto child = child::make(command_, mode);
    if (!child) {
      ctrl.abort(child.error());
      co_return;
    }
    // We yield once because reading below is blocking, but we want to
    // directly signal that our initialization is complete.
    co_yield {};
    while (child->reading()) {
      std::vector<char> buffer(block_size);
      if (auto bytes_read = child->read(as_writeable_bytes(buffer))) {
        if (bytes_read == 0) {
          co_yield {};
          continue;
        }
        buffer.resize(*bytes_read);
        auto chk = chunk::make(std::exchange(buffer, {}));
        TENZIR_DEBUG("yielding chunk with {} bytes", chk->size());
        co_yield chk;
      }
    }
    if (auto error = child->wait()) {
      ctrl.abort(std::move(error));
    }
  }

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto child = child::make(command_, stdin_mode::pipe);
    if (!child) {
      ctrl.abort(child.error());
      co_return;
    }
    // Read from child in separate thread because coroutine-based async
    // I/O is not (yet) feasible. The thread writes the chunks into a
    // queue such that to this coroutine can yield them.
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
        TENZIR_DEBUG("joining thread");
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
          co_return;
        }
        // Try yielding so far accumulated child output.
        std::unique_lock lock{chunks_mutex, std::try_to_lock};
        if (lock.owns_lock()) {
          size_t i = 0;
          auto total = chunks.size();
          while (not chunks.empty()) {
            auto chk = chunks.front();
            chunks.pop();
            TENZIR_DEBUG("yielding chunk {}/{} with {} bytes", ++i, total,
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
      TENZIR_DEBUG("yielding chunk {}/{} with {} bytes", ++i, total,
                   chk->size());
      co_yield chk;
      chunks.pop();
    }
    if (auto error = child->wait()) {
      ctrl.abort(std::move(error));
    }
  }

  auto to_string() const -> std::string override {
    return fmt::format("shell {}", escape_operator_arg(command_));
  }

  auto location() const -> operator_location override {
    // The user expectation is that shell executes relative to the
    // currently executing process.
    return operator_location::local;
  }

  auto detached() const -> bool override {
    // We may execute blocking syscalls.
    return true;
  }

  auto name() const -> std::string override {
    return "shell";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, shell_operator& x) -> bool {
    return f.apply(x.command_);
  }

private:
  std::string command_;
};

class plugin final : public virtual operator_plugin<shell_operator> {
public:
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto command = std::string{};
    auto parser = argument_parser{"shell", "https://docs.tenzir.com/next/"
                                           "operators/transformations/shell"};
    parser.add(command, "<command>");
    parser.parse(p);
    return std::make_unique<shell_operator>(std::move(command));
  }
};

} // namespace
} // namespace tenzir::plugins::shell

TENZIR_REGISTER_PLUGIN(tenzir::plugins::shell::plugin)
