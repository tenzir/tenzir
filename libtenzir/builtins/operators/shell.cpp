//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/subprocess.hpp>
#include <tenzir/async/unbounded_queue.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/co_match.hpp>
#include <tenzir/error.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution_utilities.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/coro/BoundedQueue.h>

#include <csignal>
#include <system_error>

namespace tenzir::plugins::shell {
namespace {

using namespace tenzir::binary_byte_literals;

/// The block size when reading from the child's stdout.
constexpr auto block_size = 16_KiB;

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

struct WriteFailureGraceElapsed {};

struct WriteFailure {
  std::string message;
  Option<std::error_code> code = None{};
};

using Message = variant<OutputChunk, OutputClosed, ProcessExited, TaskFailed,
                        WriteFailureGraceElapsed>;
using SourceMessageQueue = folly::coro::BoundedQueue<Message>;
using TransformMessageQueue = UnboundedQueue<Message>;

constexpr auto source_message_queue_capacity = uint32_t{16};
constexpr auto write_failure_grace_period = std::chrono::seconds{1};

auto enqueue_message(SourceMessageQueue& queue, Message message) -> Task<void> {
  co_await queue.enqueue(std::move(message));
}

auto enqueue_message(TransformMessageQueue& queue, Message message)
  -> Task<void> {
  queue.enqueue(std::move(message));
  co_return;
}

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

auto spawn_shell_subprocess(std::string command, PipeMode stdin_mode,
                            bool process_group_leader = true)
  -> Task<Subprocess> {
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
    .process_group_leader = process_group_leader,
    .kill_child_on_destruction = true,
  };
  co_return co_await Subprocess::spawn(std::move(spec));
}

template <class Queue>
auto read_stdout(Arc<Queue> queue, Subprocess& subprocess) -> Task<void> {
  auto pipe = subprocess.stdout_pipe();
  TENZIR_ASSERT(pipe.is_some());
  auto failure = Option<TaskFailed>{};
  try {
    while (true) {
      auto chunk = co_await (*pipe).read_chunk(block_size);
      if (chunk.is_none()) {
        break;
      }
      co_await enqueue_message(*queue, OutputChunk{std::move(*chunk)});
    }
    co_await enqueue_message(*queue, OutputClosed{});
  } catch (std::exception const& ex) {
    failure = TaskFailed{
      .error = ex.what(),
      .note = "failed to read from child process",
    };
  }
  if (failure.is_some()) {
    co_await enqueue_message(*queue, std::move(*failure));
  }
}

template <class Queue>
auto wait_for_exit(Arc<Queue> queue, Subprocess& subprocess) -> Task<void> {
  auto failure = Option<TaskFailed>{};
  try {
    auto return_code = co_await subprocess.wait();
    co_await enqueue_message(*queue, ProcessExited{return_code});
  } catch (std::exception const& ex) {
    failure = TaskFailed{
      .error = ex.what(),
      .note = "failed to wait for child process",
    };
  }
  if (failure.is_some()) {
    co_await enqueue_message(*queue, std::move(*failure));
  }
}

template <class Queue>
auto notify_write_failure_grace_elapsed(Arc<Queue> queue) -> Task<void> {
  co_await sleep_for(write_failure_grace_period);
  co_await enqueue_message(*queue, WriteFailureGraceElapsed{});
}

template <class Queue>
auto terminate_process_group_after_write_failure(Arc<Queue> queue,
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
    co_await enqueue_message(*queue, std::move(*failure));
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
      auto stdin_mode
        = ctx.has_terminal() ? PipeMode::inherit : PipeMode::dev_null;
      // Interactive source-mode shells must stay in the foreground process
      // group when inheriting the controlling terminal.
      auto process_group_leader = stdin_mode != PipeMode::inherit;
      subprocess_ = co_await spawn_shell_subprocess(
        std::move(*command), stdin_mode, process_group_leader);
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
      },
      [&](WriteFailureGraceElapsed) -> Task<void> {
        co_return;
      });
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
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
  mutable Arc<SourceMessageQueue> message_queue_{std::in_place,
                                                 source_message_queue_capacity};
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
      subprocess_
        = co_await spawn_shell_subprocess(std::move(*command), PipeMode::pipe);
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
      if (child_exited_ and not stdout_closed_) {
        start_write_failure_grace_timer(ctx);
      }
    } catch (std::exception const& ex) {
      lifecycle_ = Lifecycle::draining;
      if (write_failure_.is_none()) {
        write_failure_ = WriteFailure{
          .message = ex.what(),
          .code = None{},
        };
      }
      if (child_exited_ and not stdout_closed_) {
        start_write_failure_grace_timer(ctx);
      }
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
        if (write_failure_.is_some() and not stdout_closed_) {
          start_write_failure_grace_timer(ctx);
        }
        co_await finish_if_ready(ctx.dh());
      },
      [&](TaskFailed failure) -> Task<void> {
        lifecycle_ = Lifecycle::done;
        diagnostic::error("{}", failure.error)
          .note("{}", failure.note)
          .emit(ctx.dh());
        co_return;
      },
      [&](WriteFailureGraceElapsed) -> Task<void> {
        if (write_failure_.is_none()) {
          co_return;
        }
        if (stdout_closed_) {
          co_return;
        }
        start_terminate_process_group_after_write_failure(ctx);
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
    } else if (write_failure_.is_some() and child_exited_
               and not stdout_closed_) {
      start_write_failure_grace_timer(ctx);
    }
    co_return lifecycle_ == Lifecycle::done ? FinalizeBehavior::done
                                            : FinalizeBehavior::continue_;
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::normal;
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

  auto start_write_failure_grace_timer(OpCtx& ctx) -> void {
    if (write_failure_grace_timer_started_) {
      return;
    }
    write_failure_grace_timer_started_ = true;
    ctx.spawn_task(notify_write_failure_grace_elapsed(message_queue_));
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
  // `process()` writes into child stdin synchronously. Keep transform stdout
  // buffering unbounded so echo-style commands cannot deadlock writes by
  // filling a bounded queue before the executor can drain it in `process_task`.
  mutable Arc<TransformMessageQueue> message_queue_{std::in_place};
  bool stdout_closed_ = false;
  bool child_exited_ = false;
  bool exit_task_started_ = false;
  bool write_failure_grace_timer_started_ = false;
  bool termination_task_started_ = false;
  Option<std::string> exit_error_ = None{};
  Option<WriteFailure> write_failure_ = None{};
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "shell";
  }

  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto describe() const -> Description override {
    auto d = Describer<ShellArgs>{};
    // Keep the byte transform single-instance. One subprocess consumes the
    // complete stdin stream and may retain state or perform side effects;
    // scattering chunks over independent invocations would change that stream.
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
