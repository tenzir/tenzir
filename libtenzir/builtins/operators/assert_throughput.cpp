//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <folly/coro/Sleep.h>

namespace tenzir::plugins::assert_throughput {

namespace {

struct AssertThroughputArgs final {
  located<uint64_t> min_events;
  Option<located<uint64_t>> max_events;
  duration within;
  Option<uint64_t> retries;
};

auto emit_failure(AssertThroughputArgs const& args, uint64_t count,
                  bool max_exceeded, std::atomic<uint64_t>& num_failed,
                  diagnostic_handler& dh) -> void {
  auto failed = num_failed.fetch_add(1) + 1;
  auto sev = args.retries and failed > *args.retries ? severity::error
                                                     : severity::warning;
  diagnostic::builder(sev, "{}{}",
                      max_exceeded
                        ? "exceeded maximum throughput requirement"
                        : "failed to meet minimum throughput requirement",
                      failed > 1 ? fmt::format(" {} times", failed) : "")
    .note("observed {} events, expected {} {}", count,
          max_exceeded ? "at most" : "at least",
          max_exceeded ? args.max_events->inner : args.min_events.inner)
    .primary(max_exceeded ? *args.max_events : args.min_events)
    .emit(dh);
}

class AssertThroughput final : public Operator<table_slice, table_slice> {
public:
  AssertThroughput(AssertThroughputArgs args) : args_{args} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await Operator<table_slice, table_slice>::start(ctx);
    // Spawn a timer task with absolute scheduling to avoid cumulative drift.
    ctx.spawn_task([args = args_, events = num_events_, failures = num_failed_,
                    &dh = ctx.dh()] -> Task<void> {
      auto deadline = std::chrono::steady_clock::now();
      while (true) {
        deadline += args.within;
        auto now = std::chrono::steady_clock::now();
        auto remaining = deadline - now;
        if (remaining > duration::zero()) {
          co_await folly::coro::sleep(
            duration_cast<folly::HighResDuration>(remaining));
        }
        auto count = events->exchange(0);
        auto max_exceeded = args.max_events and count > args.max_events->inner;
        if (count >= args.min_events.inner and not max_exceeded) {
          failures->store(0);
          continue;
        }
        emit_failure(args, count, max_exceeded, *failures, dh);
      }
    });
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    num_events_->fetch_add(input.rows());
    co_await push(std::move(input));
  }

  auto finalize(Push<table_slice>&, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    auto count = num_events_->exchange(0);
    if (args_.max_events and count > args_.max_events->inner) {
      emit_failure(args_, count, true, *num_failed_, ctx.dh());
    }
    co_return FinalizeBehavior::done;
  }

private:
  AssertThroughputArgs args_;
  std::shared_ptr<std::atomic<uint64_t>> num_events_
    = std::make_shared<std::atomic<uint64_t>>(0);
  std::shared_ptr<std::atomic<uint64_t>> num_failed_
    = std::make_shared<std::atomic<uint64_t>>(0);
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "assert_throughput";
  }

  auto describe() const -> Description override {
    auto d = Describer<AssertThroughputArgs, AssertThroughput>{};
    auto min_events
      = d.positional("min_events", &AssertThroughputArgs::min_events);
    auto max_events = d.named("max_events", &AssertThroughputArgs::max_events);
    auto within = d.named("within", &AssertThroughputArgs::within);
    d.named("retries", &AssertThroughputArgs::retries);
    d.validate([min_events, max_events, within](DescribeCtx& ctx) -> Empty {
      TRY(auto min, ctx.get(min_events));
      auto max = ctx.get(max_events);
      if (max and max->inner < min.inner) {
        diagnostic::error("`max_events` must not be less than `min_events`")
          .primary(*max)
          .secondary(min, "`min_events`")
          .emit(ctx);
      }
      TRY(auto value, ctx.get(within));
      if (value <= duration::zero()) {
        diagnostic::error("`within` must be a positive duration")
          .primary(ctx.get_location(within).value())
          .emit(ctx);
      }
      return {};
    });
    return d.invariant_order();
  }
};

} // namespace

} // namespace tenzir::plugins::assert_throughput

TENZIR_REGISTER_PLUGIN(tenzir::plugins::assert_throughput::plugin)
