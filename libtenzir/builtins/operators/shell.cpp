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
#include <tenzir/detail/preserved_fds.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

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
    // We use `/bin/sh -c "${command}"` to interpret the command.
    auto shell = "/bin/sh";
    try {
      auto exit_handler = [](int exit, std::error_code ec) {
        TENZIR_DEBUG("child exited with code {}: {}", exit, ec.message());
      };
      switch (mode) {
        case stdin_mode::none:
          result.child_ = bp::child{
            shell,
            "-c",
            result.command_,
            bp::std_out > result.stdout_,
            bp::std_in < bp::close,
            bp::on_exit(exit_handler),
            detail::preserved_fds{{STDOUT_FILENO, STDERR_FILENO}},
            bp::detail::limit_handles_{},
          };
          break;
        case stdin_mode::inherit:
          result.child_ = bp::child{
            shell,
            "-c",
            result.command_,
            bp::std_out > result.stdout_,
            bp::on_exit(exit_handler),
            detail::preserved_fds{{STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO}},
            bp::detail::limit_handles_{},
          };
          break;
        case stdin_mode::pipe:
          result.child_ = bp::child{
            shell,
            "-c",
            result.command_,
            bp::std_out > result.stdout_,
            bp::std_in < result.stdin_,
            bp::on_exit(exit_handler),
            detail::preserved_fds{{STDIN_FILENO, STDOUT_FILENO, STDERR_FILENO}},
            bp::detail::limit_handles_{},
          };
          break;
      }
    } catch (const bp::process_error& e) {
      return caf::make_error(ec::filesystem_error, e.what());
    }
    return result;
  }

  auto read(std::span<std::byte> buffer) -> caf::expected<size_t> {
    TENZIR_ASSERT(!buffer.empty());
    TENZIR_TRACE("trying to read {} bytes", buffer.size());
    auto* data = reinterpret_cast<char*>(buffer.data());
    auto size = detail::narrow<int>(buffer.size());
    auto bytes_read = stdout_.read(data, size);
    TENZIR_TRACE("read {} bytes", bytes_read);
    return detail::narrow<size_t>(bytes_read);
  }

  auto write(std::span<const std::byte> buffer) -> caf::error {
    TENZIR_ASSERT(!buffer.empty());
    TENZIR_TRACE("writing {} bytes to child's stdin", buffer.size());
    const auto* data = reinterpret_cast<const char*>(buffer.data());
    auto size = detail::narrow_cast<std::streamsize>(buffer.size());
    if (not stdin_.write(data, size))
      return caf::make_error(ec::unspecified,
                             "failed to write into child's stdin");
    return caf::none;
  }

  void close_stdin() {
    TENZIR_DEBUG("sending EOF to child's stdin");
    stdin_.close();
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

  void terminate() {
    auto ec = std::error_code{};
    child_.terminate(ec);
    if (ec) {
      TENZIR_WARN("failed to terminate child process: {}", ec);
    }
  }

private:
  explicit child(std::string command) : command_{std::move(command)} {
    TENZIR_ASSERT(!command_.empty());
  }

  std::string command_;
  bp::child child_;
  bp::pipe stdout_;
  bp::pipe stdin_;
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
      diagnostic::error(
        add_context(child.error(), "failed to spawn child process"))
        .emit(ctrl.diagnostics());
      co_return;
    }
    // We yield once because reading below is blocking, but we want to
    // directly signal that our initialization is complete.
    co_yield {};
    auto buffer = std::vector<char>(block_size);
    while (true) {
      auto bytes_read = child->read(as_writeable_bytes(buffer));
      if (not bytes_read) {
        diagnostic::error(
          add_context(bytes_read.error(), "failed to read from child process"))
          .emit(ctrl.diagnostics());
        co_return;
      }
      if (*bytes_read == 0) {
        // Reading 0 bytes indicates EOF.
        break;
      }
      auto chk = chunk::copy(std::span{buffer.data(), *bytes_read});
      TENZIR_TRACE("yielding chunk with {} bytes", chk->size());
      co_yield chk;
    }
    if (auto error = child->wait()) {
      diagnostic::error(add_context(error, "child process execution failed"))
        .emit(ctrl.diagnostics());
      co_return;
    }
  }

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    // TODO: Handle exceptions from `boost::process`.
    auto child = child::make(command_, stdin_mode::pipe);
    if (!child) {
      diagnostic::error(
        add_context(child.error(), "failed to spawn child process"))
        .emit(ctrl.diagnostics());
      co_return;
    }
    // Read from child in separate thread because coroutine-based async
    // I/O is not (yet) feasible. The thread writes the chunks into a
    // queue such that to this coroutine can yield them.
    auto chunks = std::queue<chunk_ptr>{};
    auto chunks_mutex = std::mutex{};
    auto thread = std::thread([&child, &chunks, &chunks_mutex,
                               diagnostics = ctrl.shared_diagnostics()]() {
      try {
        auto buffer = std::vector<char>(block_size);
        while (true) {
          auto bytes_read = child->read(as_writeable_bytes(buffer));
          if (not bytes_read) {
            diagnostic::error(bytes_read.error())
              .note("failed to read from child process")
              .emit(diagnostics);
            return;
          }
          if (*bytes_read == 0) {
            // Reading 0 bytes indicates EOF.
            break;
          }
          auto chk = chunk::copy(std::span{buffer.data(), *bytes_read});
          auto lock = std::lock_guard{chunks_mutex};
          chunks.push(std::move(chk));
        }
      } catch (const std::exception& err) {
        diagnostic::error("{}", err.what())
          .note("encountered exception when reading from child process")
          .emit(diagnostics);
      }
    });
    {
      // Coroutines require RAII-style exit handling.
      auto unplanned_exit = caf::detail::make_scope_guard([&] {
        child->terminate();
        TENZIR_DEBUG("joining thread");
        thread.join();
      });
      // Loop over input chunks.
      for (auto&& chunk : input) {
        auto stalled = not chunk or chunk->size() == 0;
        if (not stalled) {
          // Pass operator input to the child's stdin.
          // TODO: If the reading end of the pipe to the child's stdin is
          // already closed, this will generate a SIGPIPE.
          if (auto err = child->write(as_bytes(*chunk))) {
            diagnostic::error(
              add_context(err, "failed to write to child process"))
              .emit(ctrl.diagnostics());
            co_return;
          }
        }
        // Try yielding so far accumulated child output.
        auto lock = std::unique_lock{chunks_mutex, std::try_to_lock};
        if (lock.owns_lock()) {
          auto i = size_t{0};
          auto total = chunks.size();
          while (not chunks.empty()) {
            auto chk = chunks.front();
            TENZIR_DEBUG("yielding chunk {}/{} with {} bytes", ++i, total,
                         chk->size());
            co_yield std::move(chk);
            chunks.pop();
          }
          if (stalled) {
            co_yield {};
          }
        } else {
          co_yield {};
        }
      }
      unplanned_exit.disable();
      child->close_stdin();
      thread.join();
      if (auto error = child->wait()) {
        diagnostic::error(add_context(error, "child process execution failed"))
          .emit(ctrl.diagnostics());
        co_return;
      }
    }
    // Yield all accumulated child output.
    auto lock = std::lock_guard{chunks_mutex};
    auto i = size_t{0};
    auto total = chunks.size();
    while (not chunks.empty()) {
      auto& chk = chunks.front();
      TENZIR_DEBUG("yielding chunk {}/{} with {} bytes", ++i, total,
                   chk->size());
      co_yield std::move(chk);
      chunks.pop();
    }
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

  auto input_independent() const -> bool override {
    // We may produce results without receiving any further input.
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

class plugin final : public virtual operator_plugin<shell_operator>,
                     public virtual operator_factory_plugin {
public:
  auto signature() const -> operator_signature override {
    return {
      .source = true,
      .transformation = true,
    };
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto command = std::string{};
    auto parser = argument_parser{"shell", "https://docs.tenzir.com/"
                                           "operators/shell"};
    parser.add(command, "<command>");
    parser.parse(p);
    return std::make_unique<shell_operator>(std::move(command));
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto command = located<std::string>{};
    auto parser
      = argument_parser2::operator_("shell").add(command, "<command>");
    TRY(parser.parse(inv, ctx));
    return std::make_unique<shell_operator>(std::move(command).inner);
  }
};

} // namespace
} // namespace tenzir::plugins::shell

TENZIR_REGISTER_PLUGIN(tenzir::plugins::shell::plugin)
