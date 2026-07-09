//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/executor.hpp>
#include <tenzir/async/semaphore.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/try.hpp>

#include <folly/CancellationToken.h>
#include <folly/coro/CurrentExecutor.h>
#include <folly/coro/Error.h>
#include <folly/coro/Sleep.h>
#include <folly/coro/WithCancellation.h>

namespace tenzir::plugins::every_cron {

namespace {

struct EveryArgs {
  duration interval = {};
  located<ir::pipeline> pipe;
};

template <class Input>
struct EveryImpl {
  explicit EveryImpl(EveryArgs args) : args_{std::move(args)} {
  }

  auto start_impl(OpCtx& ctx) -> Task<void> {
    if (next_sub_id_ == 0) {
      co_await spawn_new(ctx);
    }
  }

  auto await_task_impl() const -> Task<Any> {
    // Run the interval timer under a token that also fires when we request a
    // stop on `stop_source_`, so a stop can interrupt us even while we are
    // still waiting for the permit. Acquiring the permit outside the
    // cancellation scope would park this task on the semaphore, out of reach
    // of the stop token, whenever the permit is still held (e.g., a previous
    // interval fired but its subpipeline has not finished yet), so a
    // concurrent stop could not wake us and shutdown would hang.
    //
    // The permit is acquired only once the previous subpipeline released it,
    // so the interval timer starts after the subpipeline was actually
    // started. The time it takes to close before starting the new pipelines
    // is added on top of that. We could also aim for the total time to match
    // the specified interval, but if the subpipeline is slow to shut down
    // (e.g., `to_http` sending requests), then we run into trouble when the
    // interval is short. Hence we're going with the former, safer alternative.
    auto outer = co_await folly::coro::co_current_cancellation_token;
    auto result
      = co_await folly::coro::co_awaitTry(folly::coro::co_withCancellation(
        folly::cancellation_token_merge(outer, stop_source_.getToken()),
        folly::coro::co_invoke(
          [this, interval = args_.interval]() mutable -> Task<SemaphorePermit> {
            auto permit = co_await sleep_permits_.acquire();
            co_await sleep_for(interval);
            co_return std::move(permit);
          })));
    if (result.hasValue()) {
      co_return std::move(result).value();
    }
    // The wait was cancelled by the whole pipeline shutting down; propagate.
    if (outer.isCancellationRequested()) {
      co_yield folly::coro::co_stopped_may_throw;
    }
    co_await wait_forever();
    TENZIR_UNREACHABLE();
  }

  auto process_task_impl(Any result, OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(not sleep_done_);
    sleep_done_ = std::move(result).as<SemaphorePermit>();
    // For transformation sub-pipelines, close the current one so it
    // finishes and triggers finish_sub.
    if constexpr (not std::same_as<Input, void>) {
      TENZIR_ASSERT(next_sub_id_ > 0);
      auto sub = ctx.get_sub(next_sub_id_ - 1);
      // The pipeline might have already terminated on its own.
      if (sub) {
        auto& pipe = as<SubHandle<Input>>(*sub);
        co_await pipe.close();
      }
    }
    co_await maybe_respawn(ctx);
  }

  auto finish_sub_impl(OpCtx& ctx) -> Task<void> {
    sub_finished_ = true;
    co_await maybe_respawn(ctx);
  }

  // TODO: Clean this up with the upcoming subpipelines PR.
  auto finalize_impl() -> FinalizeBehavior {
    stop_spawning_ = true;
    // Cancel a pending `await_task` so it stops instead of arming a new timer.
    stop_source_.requestCancellation();
    return FinalizeBehavior::done;
  }

  auto stop_impl() -> void
    requires(std::same_as<Input, void>)
  {
    stop_spawning_ = true;
    stop_source_.requestCancellation();
  }

  auto state_impl() -> OperatorState {
    if (stop_spawning_ and sub_finished_) {
      return OperatorState::done;
    }
    // We want to wait for subpipeline completion when we closed the subpipeline
    // ourselves before processing further input. Note that the case where the
    // subpipeline closed itself does not block input here, because as described
    // below, the expected behavior with `head` is to just drop the data.
    return sleep_done_ ? OperatorState::blocked : OperatorState::normal;
  }

  auto process_input_impl(table_slice input, OpCtx& ctx) -> Task<void>
    requires(std::same_as<Input, table_slice>)
  {
    // This function will not be called if we are in a blocked state.
    TENZIR_ASSERT(state_impl() != OperatorState::blocked);
    TENZIR_ASSERT(next_sub_id_ > 0);
    auto sub = ctx.get_sub(int64_t{next_sub_id_ - 1});
    if (not sub) {
      // The subpipeline might already be gone. This happens if it terminates on
      // its own, for example with `head`, which implies that we can simply drop
      // the data. For the case where we ourselves close the subpipeline and
      // wait for its completion, we transition to a state that blocks the input
      // channel, which is also asserted above. So we know it's the first case.
      co_return;
    }
    auto& pipe = as<SubHandle<table_slice>>(*sub);
    // The subpipeline might have already closed its input channel, without
    // having terminated fully. The same reasoning as above applies here, so we
    // simply drop the data if we can't send it.
    std::ignore = co_await pipe.push(std::move(input));
  }

  auto snapshot_impl(Serde&) -> void {
    // TODO: I deleted the previous snapshotting implementation because it was
    // broken, and a TODO is better than a broken implementation. This needs to
    // be implemented once we want to enable checkpointing.
    TENZIR_TODO();
  }

  auto spawn_new(OpCtx& ctx) -> Task<void> {
    co_await ctx.spawn_sub<Input>(next_sub_id_, args_.pipe.inner);
    next_sub_id_ += 1;
  }

  auto maybe_respawn(OpCtx& ctx) -> Task<void> {
    if (sleep_done_ and sub_finished_ and not stop_spawning_) {
      // Release the sleep permit, which will start a new timer.
      sleep_done_ = None{};
      sub_finished_ = false;
      co_await spawn_new(ctx);
    }
  }

  EveryArgs args_;
  mutable Semaphore sleep_permits_{1};
  Option<SemaphorePermit> sleep_done_;
  int64_t next_sub_id_ = 0;
  bool sub_finished_ = false;
  bool stop_spawning_ = false;
  folly::CancellationSource stop_source_;
};

template <class Input, class Output>
class Every;

template <>
class Every<void, table_slice> final : public Operator<void, table_slice>,
                                       private EveryImpl<void> {
public:
  using EveryImpl::EveryImpl;

  auto start(OpCtx& ctx) -> Task<void> override {
    return start_impl(ctx);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    return await_task_impl();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return process_task_impl(std::move(result), ctx);
  }

  auto finish_sub(SubKeyView key, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push);
    return finish_sub_impl(ctx);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    co_return finalize_impl();
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    stop_impl();
    co_return;
  }

  auto state() -> OperatorState override {
    return state_impl();
  }

  auto snapshot(Serde& s) -> void override {
    snapshot_impl(s);
  }
};

template <>
class Every<table_slice, table_slice> final
  : public Operator<table_slice, table_slice>,
    private EveryImpl<table_slice> {
public:
  using EveryImpl::EveryImpl;

  auto start(OpCtx& ctx) -> Task<void> override {
    return start_impl(ctx);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    return await_task_impl();
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return process_input_impl(std::move(input), ctx);
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return process_task_impl(std::move(result), ctx);
  }

  auto finish_sub(SubKeyView key, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push);
    return finish_sub_impl(ctx);
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    co_return finalize_impl();
  }

  auto state() -> OperatorState override {
    return state_impl();
  }

  auto snapshot(Serde& s) -> void override {
    snapshot_impl(s);
  }
};

template <>
class Every<void, void> final : public Operator<void, void>,
                                private EveryImpl<void> {
public:
  using EveryImpl::EveryImpl;

  auto start(OpCtx& ctx) -> Task<void> override {
    return start_impl(ctx);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    return await_task_impl();
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    return process_task_impl(std::move(result), ctx);
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key);
    return finish_sub_impl(ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    co_return finalize_impl();
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    stop_impl();
    co_return;
  }

  auto state() -> OperatorState override {
    return state_impl();
  }

  auto snapshot(Serde& s) -> void override {
    snapshot_impl(s);
  }
};

template <>
class Every<table_slice, void> final : public Operator<table_slice, void>,
                                       private EveryImpl<table_slice> {
public:
  using EveryImpl::EveryImpl;

  auto start(OpCtx& ctx) -> Task<void> override {
    return start_impl(ctx);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    return await_task_impl();
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    return process_input_impl(std::move(input), ctx);
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    return process_task_impl(std::move(result), ctx);
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key);
    return finish_sub_impl(ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    co_return finalize_impl();
  }

  auto state() -> OperatorState override {
    return state_impl();
  }

  auto snapshot(Serde& s) -> void override {
    snapshot_impl(s);
  }
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.every";
  }

  auto describe() const -> Description override {
    auto d = Describer<EveryArgs>{};
    auto interval = d.positional("interval", &EveryArgs::interval);
    auto pipe = d.pipeline(&EveryArgs::pipe, SubOptimize::from_downstream);
    d.validate([interval](DescribeCtx& ctx) -> Empty {
      if (auto v = ctx.get(interval); v and *v <= duration::zero()) {
        diagnostic::error("interval must be a positive duration")
          .primary(ctx.get_location(interval).value())
          .emit(ctx);
      }
      return {};
    });
    d.spawner([pipe]<class Input>(DescribeCtx& ctx)
                -> failure_or<Option<SpawnWith<EveryArgs, Input>>> {
      if constexpr (std::same_as<Input, chunk_ptr>) {
        return {};
      } else {
        TRY(auto p, ctx.get(pipe));
        TRY(auto output, p.inner.infer_type(tag_v<Input>, ctx));
        if (not output) {
          // Output type not yet determined; default to events.
          return [](EveryArgs args) {
            return Every<Input, table_slice>{std::move(args)};
          };
        }
        return match(
          *output,
          [](tag<table_slice>)
            -> failure_or<Option<SpawnWith<EveryArgs, Input>>> {
            return [](EveryArgs args) {
              return Every<Input, table_slice>{std::move(args)};
            };
          },
          [](tag<void>) -> failure_or<Option<SpawnWith<EveryArgs, Input>>> {
            return [](EveryArgs args) {
              return Every<Input, void>{std::move(args)};
            };
          },
          [&](
            tag<chunk_ptr>) -> failure_or<Option<SpawnWith<EveryArgs, Input>>> {
            diagnostic::error("subpipeline must not produce bytes")
              .primary(p.source)
              .emit(ctx);
            return failure::promise();
          });
      }
    });
    return d.invariant_filter();
  }
};

} // namespace

} // namespace tenzir::plugins::every_cron

TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::plugin)
