//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/subprocess.hpp>
#include <tenzir/async/unbounded_queue.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/concept/parseable/string/quoted_string.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/detail/preserved_fds.hpp>
#include <tenzir/detail/scope_guard.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <boost/asio.hpp>
#if __has_include(<boost/process/v1/child.hpp>)

#  include <boost/process/v1/args.hpp>
#  include <boost/process/v1/async.hpp>
#  include <boost/process/v1/async_system.hpp>
#  include <boost/process/v1/child.hpp>
#  include <boost/process/v1/cmd.hpp>
#  include <boost/process/v1/env.hpp>
#  include <boost/process/v1/environment.hpp>
#  include <boost/process/v1/error.hpp>
#  include <boost/process/v1/exe.hpp>
#  include <boost/process/v1/group.hpp>
#  include <boost/process/v1/handles.hpp>
#  include <boost/process/v1/io.hpp>
#  include <boost/process/v1/pipe.hpp>
#  include <boost/process/v1/search_path.hpp>
#  include <boost/process/v1/shell.hpp>
#  include <boost/process/v1/spawn.hpp>
#  include <boost/process/v1/start_dir.hpp>
#  include <boost/process/v1/system.hpp>

namespace bp = boost::process::v1;

#else

#  include <boost/process.hpp>

namespace bp = boost::process;

#endif

#include <mutex>
#include <queue>
#include <string_view>
#include <system_error>
#include <thread>
#include <unistd.h>

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
    TENZIR_ASSERT(! buffer.empty());
    TENZIR_TRACE("trying to read {} bytes", buffer.size());
    auto* data = reinterpret_cast<char*>(buffer.data());
    auto size = detail::narrow<int>(buffer.size());
    auto bytes_read = stdout_.read(data, size);
    TENZIR_TRACE("read {} bytes", bytes_read);
    return detail::narrow<size_t>(bytes_read);
  }

  auto write(std::span<const std::byte> buffer) -> caf::error {
    TENZIR_ASSERT(! buffer.empty());
    TENZIR_TRACE("writing {} bytes to child's stdin", buffer.size());
    const auto* data = reinterpret_cast<const char*>(buffer.data());
    auto size = detail::narrow_cast<std::streamsize>(buffer.size());
    if (not stdin_.write(data, size)) {
      return caf::make_error(ec::unspecified,
                             "failed to write into child's stdin");
    }
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
      return diagnostic::error("{}", ec.message())
        .note("failed to wait for child process")
        .to_error();
    }
    auto code = child_.exit_code();
    if (code != 0) {
      return diagnostic::error("child process exited with exit-code {}", code)
        .to_error();
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
    TENZIR_ASSERT(! command_.empty());
  }

  std::string command_;
  bp::child child_;
  bp::pipe stdout_;
  bp::pipe stdin_;
};

class shell_operator final : public crtp_operator<shell_operator> {
public:
  shell_operator() = default;

  explicit shell_operator(located<secret> command)
    : command_{std::move(command)} {
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto command = std::string{};
    co_yield ctrl.resolve_secrets_must_yield(
      {make_secret_request("command", command_, command, ctrl.diagnostics())});
    auto mode = ctrl.has_terminal() ? stdin_mode::inherit : stdin_mode::none;
    auto child = child::make(command, mode);
    if (! child) {
      diagnostic::error(child.error())
        .note("failed to spawn child process")
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto buffer = std::vector<char>(block_size);
    while (true) {
      auto bytes_read = child->read(as_writeable_bytes(buffer));
      if (not bytes_read) {
        diagnostic::error(bytes_read.error())
          .note("failed to read from child process")
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
    if (auto error = child->wait(); error.valid()) {
      diagnostic::error(error)
        .note("child process execution failed")
        .emit(ctrl.diagnostics());
      co_return;
    }
  }

  auto operator()(generator<chunk_ptr> input,
                  operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    auto command = std::string{};
    co_yield ctrl.resolve_secrets_must_yield(
      {make_secret_request("command", command_, command, ctrl.diagnostics())});
    // TODO: Handle exceptions from `boost::process`.
    auto child = child::make(command, stdin_mode::pipe);
    if (! child) {
      diagnostic::error(child.error())
        .note("failed to spawn child process")
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
      auto unplanned_exit = detail::scope_guard([&]() noexcept {
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
          if (auto err = child->write(as_bytes(chunk)); err.valid()) {
            diagnostic::error(err)
              .note("failed to write to child process")
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
      if (auto error = child->wait(); error.valid()) {
        diagnostic::error(error)
          .note("child process execution failed")
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

  auto idle_after() const -> duration override {
    // We may produce results without receiving any further input.
    return duration::max();
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
  located<secret> command_;
};

struct ShellArgs {
  located<secret> command = located{secret{}, location::unknown};
};

struct OutputChunk {
  chunk_ptr chunk;
};

struct OutputClosed {};

struct ProcessExited {
  folly::ProcessReturnCode return_code;
};

struct TaskFailed {
  std::string error;
  std::string note;
};

struct WriteFailure {
  std::string message;
  Option<std::error_code> code = None{};
};

using Message = variant<OutputChunk, OutputClosed, ProcessExited, TaskFailed>;
// `process()` writes into child stdin synchronously. Keep stdout buffering
// unbounded so echo-style commands cannot deadlock writes by filling a bounded
// queue before the executor can drain it via `process_task()`.
using MessageQueue = UnboundedQueue<Message>;

auto resolve_command(ShellArgs const& args, OpCtx& ctx)
  -> Task<Option<std::string>> {
  auto command = std::string{};
  auto requests = std::vector<secret_request>{
    make_secret_request("command", args.command, command, ctx.dh())};
  auto result = co_await ctx.resolve_secrets(std::move(requests));
  if (not result) {
    co_return None{};
  }
  co_return command;
}

auto spawn_shell_subprocess(std::string command, bool pipe_stdin)
  -> Task<Subprocess> {
  auto stdin_mode = pipe_stdin ? PipeMode::pipe : PipeMode::dev_null;
  auto spec = SubprocessSpec{
    .argv = {"/bin/sh", "-c", std::move(command)},
    .env = None{},
    .cwd = None{},
    .stdin_mode = stdin_mode,
    .stdout_mode = PipeMode::pipe,
    .stderr_mode = PipeMode::inherit,
    .pipe_input_fds = {},
    .pipe_output_fds = {},
    .use_path = false,
    .process_group_leader = true,
    .kill_child_on_destruction = true,
  };
  co_return co_await Subprocess::spawn(std::move(spec));
}

auto read_stdout(Arc<MessageQueue> queue, Subprocess& subprocess)
  -> Task<void> {
  auto pipe = subprocess.stdout_pipe();
  TENZIR_ASSERT(pipe.is_some());
  auto failure = Option<TaskFailed>{};
  try {
    while (true) {
      auto chunk = co_await (*pipe).read_chunk(block_size);
      if (chunk.is_none()) {
        break;
      }
      co_await queue->enqueue(OutputChunk{std::move(*chunk)});
    }
    co_await queue->enqueue(OutputClosed{});
  } catch (std::exception const& ex) {
    failure = TaskFailed{
      .error = ex.what(),
      .note = "failed to read from child process",
    };
  }
  if (failure.is_some()) {
    co_await queue->enqueue(std::move(*failure));
  }
}

auto wait_for_exit(Arc<MessageQueue> queue, Subprocess& subprocess)
  -> Task<void> {
  auto failure = Option<TaskFailed>{};
  try {
    auto return_code = co_await subprocess.wait();
    co_await queue->enqueue(ProcessExited{return_code});
  } catch (std::exception const& ex) {
    failure = TaskFailed{
      .error = ex.what(),
      .note = "failed to wait for child process",
    };
  }
  if (failure.is_some()) {
    co_await queue->enqueue(std::move(*failure));
  }
}

auto terminate_process_group_after_write_failure(Arc<MessageQueue> queue,
                                                 Subprocess& subprocess)
  -> Task<void> {
  auto failure = Option<TaskFailed>{};
  try {
    co_await subprocess.send_signal_to_process_group(SIGTERM);
    co_await sleep_for(std::chrono::seconds{1});
    co_await subprocess.send_signal_to_process_group(SIGKILL);
  } catch (std::exception const& ex) {
    failure = TaskFailed{
      .error = ex.what(),
      .note = "failed to terminate child process group after stdin write "
              "failure",
    };
  }
  if (failure.is_some()) {
    co_await queue->enqueue(std::move(*failure));
  }
}

auto process_exit_error(const folly::ProcessReturnCode& return_code)
  -> Option<std::string> {
  if (return_code.exited()) {
    auto exit_code = return_code.exitStatus();
    if (exit_code == 0) {
      return None{};
    }
    return fmt::format("child process exited with exit-code {}", exit_code);
  }
  return fmt::format("child process {}", return_code.str());
}

auto format_write_failure(std::system_error const& ex) -> WriteFailure {
  return WriteFailure{
    .message = fmt::format("{} ({}: {})", ex.what(), ex.code().value(),
                           ex.code().message()),
    .code = ex.code(),
  };
}

class ShellSource final : public Operator<void, chunk_ptr> {
public:
  explicit ShellSource(ShellArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto command = co_await resolve_command(args_, ctx);
    if (not command) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    try {
      subprocess_ = co_await spawn_shell_subprocess(std::move(*command), false);
      ctx.spawn_task(read_stdout(message_queue_, *subprocess_));
      ctx.spawn_task(wait_for_exit(message_queue_, *subprocess_));
      lifecycle_ = Lifecycle::running;
    } catch (std::exception const& ex) {
      diagnostic::error("{}", ex.what())
        .note("failed to spawn child process")
        .emit(ctx.dh());
      lifecycle_ = Lifecycle::done;
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (lifecycle_ == Lifecycle::done) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    auto* message = result.try_as<Message>();
    TENZIR_ASSERT(message);
    co_await co_match(
      std::move(*message),
      [&](OutputChunk output) -> Task<void> {
        co_await push(std::move(output.chunk));
      },
      [&](OutputClosed) -> Task<void> {
        stdout_closed_ = true;
        co_await finish_if_ready(ctx.dh());
      },
      [&](ProcessExited exited) -> Task<void> {
        child_exited_ = true;
        exit_error_ = process_exit_error(exited.return_code);
        co_await finish_if_ready(ctx.dh());
      },
      [&](TaskFailed failure) -> Task<void> {
        lifecycle_ = Lifecycle::done;
        diagnostic::error("{}", failure.error)
          .note("{}", failure.note)
          .emit(ctx.dh());
        co_return;
      });
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

private:
  enum class Lifecycle {
    starting,
    running,
    done,
  };

  auto finish_if_ready(diagnostic_handler& dh) -> Task<void> {
    if (not stdout_closed_ or not child_exited_) {
      co_return;
    }
    lifecycle_ = Lifecycle::done;
    if (exit_error_) {
      diagnostic::error("{}", *exit_error_)
        .note("child process execution failed")
        .emit(dh);
    }
  }

  ShellArgs args_;
  Lifecycle lifecycle_ = Lifecycle::starting;
  Option<Subprocess> subprocess_ = None{};
  mutable Arc<MessageQueue> message_queue_{std::in_place};
  bool stdout_closed_ = false;
  bool child_exited_ = false;
  Option<std::string> exit_error_ = None{};
};

class ShellTransform final : public Operator<chunk_ptr, chunk_ptr> {
public:
  explicit ShellTransform(ShellArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto command = co_await resolve_command(args_, ctx);
    if (not command) {
      lifecycle_ = Lifecycle::done;
      co_return;
    }
    try {
      subprocess_ = co_await spawn_shell_subprocess(std::move(*command), true);
      ctx.spawn_task(read_stdout(message_queue_, *subprocess_));
      start_wait_for_exit(ctx);
      lifecycle_ = Lifecycle::running;
    } catch (std::exception const& ex) {
      diagnostic::error("{}", ex.what())
        .note("failed to spawn child process")
        .emit(ctx.dh());
      lifecycle_ = Lifecycle::done;
    }
  }

  auto process(chunk_ptr input, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    if (not input or input->size() == 0) {
      co_return;
    }
    TENZIR_ASSERT(subprocess_);
    auto stdin_pipe = subprocess_->stdin_pipe();
    TENZIR_ASSERT(stdin_pipe.is_some());
    try {
      co_await (*stdin_pipe).write(std::move(input));
    } catch (std::system_error const& ex) {
      lifecycle_ = Lifecycle::draining;
      if (write_failure_.is_none()) {
        write_failure_ = format_write_failure(ex);
      }
      start_terminate_process_group_after_write_failure(ctx);
    } catch (std::exception const& ex) {
      lifecycle_ = Lifecycle::draining;
      if (write_failure_.is_none()) {
        write_failure_ = WriteFailure{
          .message = ex.what(),
          .code = None{},
        };
      }
      start_terminate_process_group_after_write_failure(ctx);
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (lifecycle_ == Lifecycle::done) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    auto* message = result.try_as<Message>();
    TENZIR_ASSERT(message);
    co_await co_match(
      std::move(*message),
      [&](OutputChunk output) -> Task<void> {
        co_await push(std::move(output.chunk));
      },
      [&](OutputClosed) -> Task<void> {
        stdout_closed_ = true;
        co_await finish_if_ready(ctx.dh());
      },
      [&](ProcessExited exited) -> Task<void> {
        child_exited_ = true;
        exit_error_ = process_exit_error(exited.return_code);
        co_await finish_if_ready(ctx.dh());
      },
      [&](TaskFailed failure) -> Task<void> {
        lifecycle_ = Lifecycle::done;
        diagnostic::error("{}", failure.error)
          .note("{}", failure.note)
          .emit(ctx.dh());
        co_return;
      });
  }

  auto finalize(Push<chunk_ptr>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    if (lifecycle_ == Lifecycle::done) {
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining;
      TENZIR_ASSERT(subprocess_);
      auto stdin_pipe = subprocess_->stdin_pipe();
      TENZIR_ASSERT(stdin_pipe.is_some());
      if (not(*stdin_pipe).is_closed()) {
        try {
          co_await (*stdin_pipe).close();
        } catch (std::exception const& ex) {
          lifecycle_ = Lifecycle::done;
          diagnostic::error("{}", ex.what())
            .note("failed to close child process stdin")
            .emit(ctx.dh());
          co_return FinalizeBehavior::done;
        }
      }
      start_wait_for_exit(ctx);
    } else if (write_failure_.is_some()) {
      start_terminate_process_group_after_write_failure(ctx);
    }
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

private:
  enum class Lifecycle {
    starting,
    running,
    draining,
    done,
  };

  auto start_wait_for_exit(OpCtx& ctx) -> void {
    TENZIR_ASSERT(subprocess_);
    if (exit_task_started_) {
      return;
    }
    exit_task_started_ = true;
    ctx.spawn_task(wait_for_exit(message_queue_, *subprocess_));
  }

  auto start_terminate_process_group_after_write_failure(OpCtx& ctx) -> void {
    TENZIR_ASSERT(subprocess_);
    if (termination_task_started_) {
      return;
    }
    termination_task_started_ = true;
    ctx.spawn_task(terminate_process_group_after_write_failure(message_queue_,
                                                               *subprocess_));
  }

  auto finish_if_ready(diagnostic_handler& dh) -> Task<void> {
    if (not stdout_closed_ or not child_exited_) {
      co_return;
    }
    lifecycle_ = Lifecycle::done;
    auto write_failure = write_failure_;
    if (write_failure.is_some() and write_failure->code.is_some()
        and *write_failure->code == std::errc::broken_pipe and exit_error_) {
      write_failure = None{};
    }
    if (exit_error_) {
      if (write_failure.is_some()) {
        diagnostic::error("{}", *exit_error_)
          .note("child process execution failed")
          .note("failed to write to child process: {}", write_failure->message)
          .emit(dh);
      } else {
        diagnostic::error("{}", *exit_error_)
          .note("child process execution failed")
          .emit(dh);
      }
    } else if (write_failure.is_some()) {
      diagnostic::error("{}", write_failure->message)
        .note("failed to write to child process")
        .emit(dh);
    }
  }

  ShellArgs args_;
  Lifecycle lifecycle_ = Lifecycle::starting;
  Option<Subprocess> subprocess_ = None{};
  mutable Arc<MessageQueue> message_queue_{std::in_place};
  bool stdout_closed_ = false;
  bool child_exited_ = false;
  bool exit_task_started_ = false;
  bool termination_task_started_ = false;
  Option<std::string> exit_error_ = None{};
  Option<WriteFailure> write_failure_ = None{};
};

class plugin final : public virtual operator_plugin2<shell_operator>,
                     public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "shell";
  }

  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto command = located<secret>{};
    auto parser
      = argument_parser2::operator_("shell").positional("cmd", command);
    TRY(parser.parse(inv, ctx));
    return std::make_unique<shell_operator>(std::move(command));
  }

  auto describe() const -> Description override {
    auto d = Describer<ShellArgs>{};
    d.positional("cmd", &ShellArgs::command);
    d.spawner([]<class Input>(DescribeCtx&)
                -> failure_or<Option<SpawnWith<ShellArgs, Input>>> {
      if constexpr (std::same_as<Input, void>) {
        return SpawnWith<ShellArgs, void>{
          [](ShellArgs args) -> Box<Operator<void, chunk_ptr>> {
            return Box<Operator<void, chunk_ptr>>{ShellSource{std::move(args)}};
          }};
      } else if constexpr (std::same_as<Input, chunk_ptr>) {
        return SpawnWith<ShellArgs, chunk_ptr>{
          [](ShellArgs args) -> Box<Operator<chunk_ptr, chunk_ptr>> {
            return Box<Operator<chunk_ptr, chunk_ptr>>{
              ShellTransform{std::move(args)}};
          }};
      } else {
        return None{};
      }
    });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::shell

TENZIR_REGISTER_PLUGIN(tenzir::plugins::shell::plugin)
