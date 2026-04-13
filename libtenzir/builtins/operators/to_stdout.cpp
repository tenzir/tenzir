//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async.hpp"
#include "tenzir/async/blocking_executor.hpp"

#include <tenzir/detail/posix.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/substitute_ctx.hpp>

#include <folly/coro/BoundedQueue.h>

#include <cerrno>
#include <cstdio>

namespace tenzir::plugins::to_stdout {

namespace {

constexpr auto message_queue_capacity = uint32_t{16};

struct ToStdoutArgs {
  location self;
  located<ir::pipeline> pipe;
};

class ToStdout final : public Operator<table_slice, void> {
public:
  enum class Lifecycle {
    running,
    draining,
    done,
  };

  explicit ToStdout(ToStdoutArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto pipe = std::move(args_.pipe.inner);
    if (not pipe.substitute(substitute_ctx{{ctx}, nullptr}, true)) {
      diagnostic::error("failed to substitute pipeline")
        .primary(args_.pipe)
        .emit(ctx);
      finish();
      co_return;
    }
    co_await ctx.spawn_sub<table_slice>(caf::none, std::move(pipe));
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ != Lifecycle::running) {
      co_return;
    }
    auto sub = ctx.get_sub(caf::none);
    if (not sub) {
      finish();
      co_return;
    }
    auto& pipeline = as<SubHandle<table_slice>>(*sub);
    auto result = co_await pipeline.push(std::move(input));
    if (result.is_err()) {
      finish();
    }
  }

  auto process_sub(SubKeyView, chunk_ptr chunk, OpCtx&) -> Task<void> override {
    if (not chunk or chunk->size() == 0) {
      co_return;
    }
    co_await message_queue_->enqueue(std::move(chunk));
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return chunk_ptr{};
    }
    co_return co_await message_queue_->dequeue();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    auto* chunk = result.try_as<chunk_ptr>();
    TENZIR_ASSERT(chunk);
    if (not *chunk) {
      finish();
      co_return;
    }
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    co_await write_chunk(*chunk, ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (lifecycle_ == Lifecycle::done) {
      co_await flush_stdout(ctx);
      co_return FinalizeBehavior::done;
    }
    if (lifecycle_ == Lifecycle::running) {
      lifecycle_ = Lifecycle::draining;
      if (auto sub = ctx.get_sub(caf::none)) {
        auto& pipeline = as<SubHandle<table_slice>>(*sub);
        co_await pipeline.close();
        co_return FinalizeBehavior::continue_;
      }
      finish();
      co_await flush_stdout(ctx);
      co_return FinalizeBehavior::done;
    }
    co_return FinalizeBehavior::continue_;
  }

  auto finish_sub(SubKeyView, OpCtx& ctx) -> Task<void> override {
    if (lifecycle_ == Lifecycle::done) {
      co_return;
    }
    ctx.spawn_task([this]() -> Task<void> {
      co_await message_queue_->enqueue(chunk_ptr{});
    });
    co_return;
  }

  auto state() -> OperatorState override {
    return lifecycle_ == Lifecycle::done ? OperatorState::done
                                         : OperatorState::unspecified;
  }

private:
  using MessageQueue = folly::coro::BoundedQueue<chunk_ptr>;

  auto write_chunk(chunk_ptr const& chunk, OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(chunk);
    auto err = co_await spawn_blocking([chunk] {
      auto written = std::fwrite(chunk->data(), 1, chunk->size(), stdout);
      return written != chunk->size() ? errno : 0;
    });
    if (err != 0) {
      diagnostic::error("failed to write to stdout: {}",
                        detail::describe_errno(err))
        .primary(args_.self)
        .emit(ctx);
      finish();
    }
  }

  auto flush_stdout(OpCtx& ctx) -> Task<void> {
    auto err = co_await spawn_blocking([] {
      return std::fflush(stdout) != 0 ? errno : 0;
    });
    if (err != 0) {
      diagnostic::warning("failed to flush stdout: {}",
                          detail::describe_errno(err))
        .primary(args_.self)
        .emit(ctx);
    }
  }

  auto finish() -> void {
    lifecycle_ = Lifecycle::done;
  }

  ToStdoutArgs args_;
  mutable Box<MessageQueue> message_queue_{std::in_place,
                                           message_queue_capacity};
  Lifecycle lifecycle_ = Lifecycle::running;
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
      TRY(auto pipe, ctx.get(pipe_arg));
      auto output = pipe.inner.infer_type(tag_v<table_slice>, ctx);
      if (output.is_error()) {
        return {};
      }
      if (not *output or (*output)->is_not<chunk_ptr>()) {
        diagnostic::error("pipeline must return bytes")
          .primary(pipe.source.subloc(0, 1))
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::to_stdout

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_stdout::ToStdoutPlugin)
