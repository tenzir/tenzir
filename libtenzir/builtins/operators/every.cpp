//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/task.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/try.hpp>

#include <folly/coro/Sleep.h>

namespace tenzir::plugins::every_cron {

namespace {

using std::chrono::steady_clock;

struct EveryArgs {
  duration interval = {};
  located<ir::pipeline> pipe;
};

template <class Input>
struct EveryImpl {
  explicit EveryImpl(EveryArgs args) : args_{std::move(args)} {
  }

  auto start_impl(OpCtx& ctx) -> Task<void> {
    if (next_ == 0) {
      co_await spawn_new(ctx);
    }
  }

  auto await_task_impl() const -> Task<Any> {
    auto next = last_started_ + args_.interval;
    co_await sleep_until(next);
    co_return {};
  }

  auto process_task_impl(OpCtx& ctx) -> Task<void> {
    if (sleep_done_) {
      co_return;
    }
    sleep_done_ = true;
    // For transformation sub-pipelines, close the current one so it
    // finishes and triggers finish_sub.
    if constexpr (not std::same_as<Input, void>) {
      TENZIR_ASSERT(next_ > 0);
      auto sub = ctx.get_sub(next_ - 1);
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
    done_ = true;
    return FinalizeBehavior::done;
  }

  auto process_input_impl(table_slice input, OpCtx& ctx) -> Task<void>
    requires(std::same_as<Input, table_slice>)
  {
    TENZIR_ASSERT(next_ > 0);
    // TODO: This can drop events at the boundary between closing the old
    // sub-pipeline and spawning the new one.
    auto sub = ctx.get_sub(int64_t{next_ - 1});
    if (not sub) {
      TENZIR_WARN("every: dropping {} rows; sub-pipeline not available",
                  input.rows());
      co_return;
    }
    auto& pipe = as<SubHandle<table_slice>>(*sub);
    std::ignore = co_await pipe.push(std::move(input));
  }

  auto snapshot_impl(Serde& s) -> void {
    s("next", next_);
  }

  auto spawn_new(OpCtx& ctx) -> Task<void> {
    last_started_ = steady_clock::now();
    sleep_done_ = false;
    sub_finished_ = false;
    co_await ctx.spawn_sub<Input>(next_, args_.pipe.inner);
    ++next_;
  }

  auto maybe_respawn(OpCtx& ctx) -> Task<void> {
    if (sleep_done_ and sub_finished_ and not done_) {
      co_await spawn_new(ctx);
    }
  }

  EveryArgs args_;
  steady_clock::time_point last_started_ = steady_clock::time_point::min();
  int64_t next_ = 0;
  bool sleep_done_ = false;
  bool sub_finished_ = false;
  bool done_ = false;
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
    TENZIR_UNUSED(result, push);
    return process_task_impl(ctx);
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
    TENZIR_UNUSED(result, push);
    return process_task_impl(ctx);
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
    TENZIR_UNUSED(result);
    return process_task_impl(ctx);
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key);
    return finish_sub_impl(ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    co_return finalize_impl();
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
    TENZIR_UNUSED(result);
    return process_task_impl(ctx);
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key);
    return finish_sub_impl(ctx);
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    co_return finalize_impl();
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
    auto pipe = d.pipeline(&EveryArgs::pipe);
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
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::every_cron

TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::plugin)
