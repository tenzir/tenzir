//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/task.hpp>
#include <tenzir/detail/croncpp.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>

#include <folly/coro/Sleep.h>

#include <chrono>

namespace tenzir::plugins::cron {

namespace {

using std::chrono::system_clock;

auto sleep_until_next_cron(const detail::cron::cronexpr& expr) -> Task<void> {
  auto now = system_clock::now();
  auto next_ts = detail::cron::cron_next(expr, now);

  // The check is needed because `-` can overflow and yield unexpected results.
  auto duration = next_ts < now ? system_clock::duration{0} : next_ts - now;
  return sleep_for(duration);
}

struct CronArgs {
  located<std::string> schedule;
  located<ir::pipeline> pipe;
};

class Cron : public Operator<void, table_slice> {
public:
  explicit Cron(CronArgs args) : args_{std::move(args)} {
    cronexpr_ = detail::cron::make_cron(args_.schedule.inner);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_await sleep_until_next_cron(cronexpr_);
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result, push);
    cron_fired_ = true;
    if (sub_finished_ and not done_) {
      co_await spawn(ctx);
    }
  }

  auto finish_sub(SubKeyView key, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push);
    sub_finished_ = true;
    if (cron_fired_ and not done_) {
      co_await spawn(ctx);
    }
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push, ctx);
    done_ = true;
    co_return FinalizeBehavior::done;
  }

protected:
  auto spawn(OpCtx& ctx) -> Task<void> {
    cron_fired_ = false;
    sub_finished_ = false;
    co_await ctx.spawn_sub<void>(next_sub_key_++, args_.pipe.inner);
  }

private:
  CronArgs args_;
  detail::cron::cronexpr cronexpr_;
  int64_t next_sub_key_ = 0;
  bool cron_fired_ = false;
  bool sub_finished_ = true;
  bool done_ = false;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.cron";
  }

  auto describe() const -> Description override {
    auto d = Describer<CronArgs, Cron>{};
    auto schedule = d.positional("schedule", &CronArgs::schedule, "string");
    d.pipeline(&CronArgs::pipe);
    d.validate([schedule](DescribeCtx& ctx) -> Empty {
      if (auto v = ctx.get(schedule)) {
        try {
          detail::cron::make_cron(v->inner);
        } catch (const detail::cron::bad_cronexpr& ex) {
          auto msg = std::string_view{ex.what()};
          if (msg.contains("stoul")) {
            diagnostic::error(
              "bad cron expression: invalid value for at least one field")
              .primary(v->source)
              .emit(ctx);
          } else {
            diagnostic::error("bad cron expression: {}", msg)
              .primary(v->source)
              .emit(ctx);
          }
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::cron

TENZIR_REGISTER_PLUGIN(tenzir::plugins::cron::plugin)
