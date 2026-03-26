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
class EveryBase : public Operator<Input, table_slice> {
public:
  explicit EveryBase(EveryArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (next_ == 0) {
      co_await spawn_new(ctx);
    }
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    auto next = last_started_ + args_.interval;
    co_await sleep_until(next);
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result, push);
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

  auto finish_sub(SubKeyView key, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push);
    sub_finished_ = true;
    co_await maybe_respawn(ctx);
  }

  // TODO: Clean this up with the upcoming subpipelines PR.
  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    done_ = true;
    co_return FinalizeBehavior::done;
  }

  auto snapshot(Serde& s) -> void override {
    s("next", next_);
  }

protected:
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

  auto next() const {
    return next_;
  }

private:
  EveryArgs args_;
  steady_clock::time_point last_started_ = steady_clock::time_point::min();
  int64_t next_ = 0;
  bool sleep_done_ = false;
  bool sub_finished_ = false;
  bool done_ = false;
};

template <class Input>
class Every;

template <>
class Every<table_slice> final : public EveryBase<table_slice> {
public:
  using EveryBase::EveryBase;

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    TENZIR_ASSERT(this->next() > 0);
    // TODO: This can drop events at the boundary between closing the old
    // sub-pipeline and spawning the new one.
    auto sub = ctx.get_sub(int64_t{this->next() - 1});
    if (not sub) {
      TENZIR_WARN("every: dropping {} rows; sub-pipeline not available",
                  input.rows());
      co_return;
    }
    auto& pipe = as<SubHandle<table_slice>>(*sub);
    std::ignore = co_await pipe.push(std::move(input));
  }
};

template <>
class Every<void> final : public EveryBase<void> {
public:
  using EveryBase<void>::EveryBase;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.every";
  }

  auto describe() const -> Description override {
    auto d = Describer<EveryArgs, Every<void>, Every<table_slice>>{};
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
        if (output and *output != tag_v<table_slice>) {
          diagnostic::error("subpipeline must produce events")
            .primary(p.source)
            .emit(ctx);
          return failure::promise();
        }
        return std::function<Box<Operator<Input, table_slice>>(EveryArgs)>{
          [](EveryArgs args) {
            return Every<Input>{std::move(args)};
          }};
      }
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::every_cron

TENZIR_REGISTER_PLUGIN(tenzir::plugins::every_cron::plugin)
