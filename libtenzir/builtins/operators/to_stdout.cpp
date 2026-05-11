//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"

#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/posix.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/session.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/parser.hpp>

#include <folly/CancellationToken.h>
#include <folly/coro/Baton.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/Error.h>
#include <folly/io/IOBuf.h>
#include <folly/io/async/AsyncPipe.h>
#include <folly/io/async/AsyncSocketException.h>
#include <folly/net/NetworkSocket.h>

#include <fcntl.h>
#include <memory>
#include <string>
#include <unistd.h>

namespace tenzir::plugins::to_stdout {

namespace {

constexpr auto error_queue_capacity = uint32_t{1};

struct ToStdoutArgs {
  location self;
  Option<located<ir::pipeline>> pipe;
};

struct SaveStdoutArgs {
  location self;
};

auto make_default_printer(base_ctx ctx) -> failure_or<ir::pipeline> {
  auto sessions = session_provider::make(static_cast<diagnostic_handler&>(ctx));
  TRY(auto pipe, parse("write_tql", sessions.as_session()));
  auto root = compile_ctx::make_root(ctx);
  return std::move(pipe).compile(root);
}

class StdoutNonblockingGuard {
public:
  StdoutNonblockingGuard() = default;
  StdoutNonblockingGuard(StdoutNonblockingGuard const&) = delete;
  auto operator=(StdoutNonblockingGuard const&)
    -> StdoutNonblockingGuard& = delete;
  StdoutNonblockingGuard(StdoutNonblockingGuard&& other) noexcept
    : active_{std::exchange(other.active_, false)},
      orig_flags_{std::exchange(other.orig_flags_, -1)} {
  }
  auto operator=(StdoutNonblockingGuard&& other) noexcept
    -> StdoutNonblockingGuard& {
    if (this != &other) {
      restore();
      active_ = std::exchange(other.active_, false);
      orig_flags_ = std::exchange(other.orig_flags_, -1);
    }
    return *this;
  }
  ~StdoutNonblockingGuard() {
    restore();
  }

  auto activate() -> Option<std::string> {
    TENZIR_ASSERT(not active_);
    auto orig_flags = ::fcntl(STDOUT_FILENO, F_GETFL, 0);
    if (orig_flags < 0) {
      return fmt::format("failed to get stdout flags: {}",
                         detail::describe_errno());
    }
    if (::fcntl(STDOUT_FILENO, F_SETFL, orig_flags | O_NONBLOCK) < 0) {
      return fmt::format("failed to enable non-blocking mode for stdout: {}",
                         detail::describe_errno());
    }
    active_ = true;
    orig_flags_ = orig_flags;
    return {};
  }

  auto restore() -> void {
    if (not active_) {
      return;
    }
    TENZIR_ASSERT(orig_flags_ >= 0);
    ::fcntl(STDOUT_FILENO, F_SETFL, orig_flags_);
    active_ = false;
    orig_flags_ = -1;
  }

private:
  bool active_ = false;
  int orig_flags_ = -1;
};

auto write_chunk(folly::AsyncPipeWriter& writer, chunk_ptr chunk)
  -> Task<Option<std::string>> {
  using ResultQueue = folly::coro::BoundedQueue<Option<std::string>>;

  class WriteCallback final : public folly::AsyncWriter::WriteCallback {
  public:
    explicit WriteCallback(std::shared_ptr<ResultQueue> result_queue)
      : result_queue_{std::move(result_queue)} {
    }

    void writeSuccess() noexcept override {
      std::ignore = result_queue_->try_enqueue(Option<std::string>{});
      delete this;
    }

    void
    writeErr(size_t, folly::AsyncSocketException const& ex) noexcept override {
      std::ignore = result_queue_->try_enqueue(Option<std::string>{ex.what()});
      delete this;
    }

  private:
    std::shared_ptr<ResultQueue> result_queue_;
  };

  TENZIR_ASSERT(chunk);
  auto result_queue = std::make_shared<ResultQueue>(1);
  auto write_callback = std::make_unique<WriteCallback>(result_queue);
  auto buffer = folly::IOBuf::copyBuffer(chunk->data(), chunk->size());
  writer.write(std::move(buffer), write_callback.get());
  std::ignore = write_callback.release();
  co_return co_await result_queue->dequeue();
}

class ToStdout final : public Operator<table_slice, void> {
public:
  explicit ToStdout(ToStdoutArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto pipe = ir::pipeline{};
    if (args_.pipe) {
      pipe = std::move(args_.pipe->inner);
    } else {
      auto default_printer = make_default_printer(ctx);
      if (not default_printer) {
        done_ = true;
        co_return;
      }
      pipe = std::move(*default_printer);
    }
    if (not pipe.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      diagnostic::error("failed to substitute pipeline")
        .primary(args_.pipe ? args_.pipe->source : args_.self)
        .emit(ctx);
      done_ = true;
      co_return;
    }
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_stdout"},
                         MetricsDirection::write, MetricsVisibility::external_);
    if (auto error = stdout_nonblocking_.activate()) {
      diagnostic::error("{}", *error).primary(args_.self).emit(ctx);
      done_ = true;
      co_return;
    }
    io_executor_ = ctx.io_executor();
    writer_ = folly::AsyncPipeWriter::newWriter(
      io_executor_->getEventBase(),
      folly::NetworkSocket::fromFd(STDOUT_FILENO));
    // Avoid closing the process-global stdout when the writer shuts down.
    writer_->setCloseCallback([](folly::NetworkSocket) {});
    co_await ctx.spawn_sub<table_slice>(caf::none, std::move(pipe));
    TENZIR_ASSERT(ctx.get_sub(caf::none));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto* error = result.try_as<std::string>();
    TENZIR_ASSERT(error);
    diagnostic::error("failed to write to stdout: {}", *error)
      .primary(args_.self)
      .emit(ctx);
    done_ = true;
    co_return;
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (done_) {
      co_return;
    }
    auto sub = ctx.get_sub(caf::none);
    if (not sub) {
      done_ = true;
      co_return;
    }
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    auto push_result = co_await pipeline.push(std::move(input));
    if (push_result.is_err()) {
      done_ = true;
    }
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (not chunk or chunk->size() == 0 or not writer_) {
      co_return;
    }
    const auto bytes = chunk->size();
    auto error = co_await folly::coro::co_withExecutor(
      io_executor_, write_chunk(*writer_, std::move(chunk)));
    if (error) {
      auto write_error = std::move(*error);
      // Only the first write error matters; blocking here would let concurrent
      // `process_sub()` calls stall behind an already queued error.
      std::ignore = queue_->try_enqueue(std::move(write_error));
    } else {
      bytes_write_counter_.add(bytes);
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (auto sub = ctx.get_sub(caf::none)) {
      auto& pipeline = as<SubHandle<table_slice>>(*sub);
      co_await pipeline.close();
      // `continue_` keeps the executor alive until the subpipeline drains and
      // reports `finish_sub()`.
      co_return FinalizeBehavior::continue_;
    }
    writer_.reset();
    stdout_nonblocking_.restore();
    co_return FinalizeBehavior::done;
  }

  auto finish_sub(SubKeyView, OpCtx&) -> Task<void> override {
    writer_.reset();
    stdout_nonblocking_.restore();
    done_ = true;
    co_return;
  }

private:
  using ErrorQueue = folly::coro::BoundedQueue<std::string>;

  ToStdoutArgs args_;
  folly::Executor::KeepAlive<folly::IOExecutor> io_executor_;
  StdoutNonblockingGuard stdout_nonblocking_;
  folly::AsyncPipeWriter::UniquePtr writer_;
  // `process_sub()` reports the first stdout write error back to the main
  // loop, which owns the operator lifecycle state.
  mutable Box<ErrorQueue> queue_{std::in_place, error_queue_capacity};
  MetricsCounter bytes_write_counter_;
  bool done_ = false;
};

class SaveStdout final : public Operator<chunk_ptr, void> {
public:
  explicit SaveStdout(SaveStdoutArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "save_stdout"},
                         MetricsDirection::write, MetricsVisibility::external_);
    if (auto error = stdout_nonblocking_.activate()) {
      diagnostic::error("{}", *error).primary(args_.self).emit(ctx);
      done_ = true;
      co_return;
    }
    io_executor_ = ctx.io_executor();
    writer_ = folly::AsyncPipeWriter::newWriter(
      io_executor_->getEventBase(),
      folly::NetworkSocket::fromFd(STDOUT_FILENO));
    // Avoid closing the process-global stdout when the writer shuts down.
    writer_->setCloseCallback([](folly::NetworkSocket) {});
    co_return;
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    co_return co_await queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto* error = result.try_as<std::string>();
    TENZIR_ASSERT(error);
    diagnostic::error("failed to write to stdout: {}", *error)
      .primary(args_.self)
      .emit(ctx);
    done_ = true;
    co_return;
  }

  auto process(chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (done_ or not chunk or chunk->size() == 0 or not writer_) {
      co_return;
    }
    const auto bytes = chunk->size();
    auto error = co_await folly::coro::co_withExecutor(
      io_executor_, write_chunk(*writer_, std::move(chunk)));
    if (error) {
      auto write_error = std::move(*error);
      // Only the first write error matters; blocking here would let concurrent
      // `process()` calls stall behind an already queued error.
      std::ignore = queue_->try_enqueue(std::move(write_error));
    } else {
      bytes_write_counter_.add(bytes);
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto finalize(OpCtx&) -> Task<FinalizeBehavior> override {
    writer_.reset();
    stdout_nonblocking_.restore();
    co_return FinalizeBehavior::done;
  }

private:
  using ErrorQueue = folly::coro::BoundedQueue<std::string>;

  SaveStdoutArgs args_;
  folly::Executor::KeepAlive<folly::IOExecutor> io_executor_;
  StdoutNonblockingGuard stdout_nonblocking_;
  folly::AsyncPipeWriter::UniquePtr writer_;
  // `process()` reports the first stdout write error back to the main loop,
  // which owns the operator lifecycle state.
  mutable Box<ErrorQueue> queue_{std::in_place, error_queue_capacity};
  MetricsCounter bytes_write_counter_;
  bool done_ = false;
};

class SaveStdoutPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.save_stdout";
  }

  auto describe() const -> Description override {
    auto d = Describer<SaveStdoutArgs, SaveStdout>{};
    d.operator_location(&SaveStdoutArgs::self);
    return d.without_optimize();
  }
};

class ToStdoutPlugin final : public OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "to_stdout";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToStdoutArgs, ToStdout>{};
    d.operator_location(&ToStdoutArgs::self);
    auto pipe_arg = d.pipeline(&ToStdoutArgs::pipe);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      auto pipe = ctx.get(pipe_arg);
      if (not pipe) {
        return {};
      }
      auto output = pipe->inner.infer_type(tag_v<table_slice>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<chunk_ptr>()) {
        diagnostic::error("pipeline must return bytes")
          .primary(pipe->source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::to_stdout

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_stdout::SaveStdoutPlugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_stdout::ToStdoutPlugin)
