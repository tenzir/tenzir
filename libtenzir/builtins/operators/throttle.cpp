//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/weak_run_delayed.hpp"

#include <tenzir/async.hpp>
#include <tenzir/async/channel.hpp>
#include <tenzir/async/metrics.hpp>
#include <tenzir/async/result.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/checked_math.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view.hpp>

#include <arrow/type.h>
#include <folly/CancellationToken.h>

#include <chrono>
#include <vector>

namespace tenzir::plugins::throttle {

struct throttle_args final {
  located<uint64_t> rate;
  located<duration> window{std::chrono::seconds{1}, location::unknown};
  ast::expression weight{ast::constant{uint64_t{1}, location::unknown}};
  Option<location> drop;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (rate.inner == 0) {
      diagnostic::error("`rate` must be a positive value")
        .primary(rate)
        .emit(dh);
      return failure::promise();
    }
    if (window.inner <= duration::zero()) {
      diagnostic::error("`window` must be a positive duration")
        .primary(window)
        .emit(dh);
      return failure::promise();
    }

    return {};
  }

  friend auto inspect(auto& f, throttle_args& x) -> bool {
    return f.object(x).fields(f.field("rate", x.rate),
                              f.field("window", x.window),
                              f.field("weight", x.weight),
                              f.field("drop", x.drop));
  }
};

struct ThrottleArgs {
  located<uint64_t> rate;
  located<duration> window{std::chrono::seconds{1}, location::unknown};
  ast::expression weight{ast::constant{uint64_t{1}, location::unknown}};
  Option<location> drop;
};

template <class Events>
class Throttle final : public Operator<Events, Events> {
public:
  explicit Throttle(ThrottleArgs args) : args_{std::move(args)} {
    auto [sender, receiver] = channel<std::chrono::steady_clock::time_point>(1);
    timer_sender_ = std::move(sender);
    timer_receiver_
      = std::make_shared<Receiver<std::chrono::steady_clock::time_point>>(
        std::move(receiver));
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if constexpr (std::same_as<Events, nova::Events>) {
      auto evaluator = nova::Evaluator::make(
        args_.weight, nova::InstantiateCtx{ctx.dh(), ctx.reg()});
      if (not evaluator) {
        co_return;
      }
      evaluator_.emplace(std::move(*evaluator));
    }
    if (args_.drop) {
      throttle_metrics_
        = make_metric_handler(ctx, type{
                                     "tenzir.metrics.throttle",
                                     record_type{
                                       {"dropped_events", int64_type{}},
                                     },
                                   });
      last_emit_ = std::chrono::steady_clock::now();
    }
    co_return;
  }

  auto process(Events input, Push<Events>& push, OpCtx& ctx)
    -> Task<void> override {
    // While input is stashed in `pending_`, `state()` reports blocked, so the
    // executor defers further input until the pacing timer released it.
    TENZIR_ASSERT(not pending_);
    co_await emit(std::move(input), push, ctx);
  }

  // Pacing runs through `await_task()` instead of sleeping in `process()`:
  // the executor's main loop awaits `process()` inline, so a sleep there
  // blocks control messages and a graceful stop could not interrupt the wait.
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    auto deadline = co_await timer_receiver_->recv();
    if (deadline.is_none()) {
      co_return TimerFired{};
    }
    // Sleep until the window rolls over, but let `stop()` interrupt the wait
    // so that stashed input drains promptly during graceful shutdown.
    auto token = folly::cancellation_token_merge(
      co_await folly::coro::co_current_cancellation_token,
      stop_cancel_.getToken());
    auto result = co_await async_result(
      folly::coro::co_withCancellation(token, sleep_until(*deadline)));
    TENZIR_UNUSED(result);
    co_return TimerFired{};
  }

  auto process_task(Any result, Push<Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not result.try_as<TimerFired>() or not pending_) {
      co_return;
    }
    auto pending = std::move(*pending_);
    pending_ = None{};
    start_ = std::chrono::steady_clock::now();
    if (pending.ranges.empty()) {
      total_ = 0;
      co_await emit(std::move(pending.input), push, ctx);
      co_return;
    }
    if (stopping_) {
      auto begin = pending.ranges[pending.next].begin;
      auto end = pending.ranges.back().end;
      co_await push(select_range(std::move(pending.input), begin, end));
      co_return;
    }
    auto range = pending.ranges[pending.next];
    co_await push(select_range(pending.input, range.begin, range.end));
    ++pending.next;
    if (pending.next == pending.ranges.size()) {
      total_ = pending.final_total;
      co_return;
    }
    total_ = args_.rate.inner;
    pending_ = std::move(pending);
    arm_timer(*start_ + args_.window.inner);
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    // Stop pacing so that stashed input drains promptly. Keeping the rate
    // limit during shutdown would hold the pipeline open until the grace
    // period force-cancels it, losing the stashed events entirely.
    stopping_ = true;
    stop_cancel_.requestCancellation();
    co_return;
  }

  auto state() -> OperatorState override {
    return pending_ ? OperatorState::blocked : OperatorState::normal;
  }

private:
  struct TimerFired {};

  struct PhysicalRange {
    size_t begin;
    size_t end;
  };

  struct Pending {
    Events input;
    std::vector<PhysicalRange> ranges;
    size_t next;
    uint64_t final_total;
  };

  auto emit(Events input, Push<Events>& push, OpCtx& ctx) -> Task<void> {
    auto now = std::chrono::steady_clock::now();
    if (args_.drop and last_emit_
        and now - *last_emit_ >= std::chrono::seconds{1}) {
      last_emit_ = now;
      if (dropped_events_ > 0) {
        throttle_metrics_.emit({{"dropped_events", int64_t(dropped_events_)}});
        dropped_events_ = 0;
      }
    }
    if (not start_) {
      start_ = now;
    }
    if (now - *start_ >= args_.window.inner) {
      start_ = now;
      total_ = 0;
    }
    if (stopping_ and not args_.drop) {
      co_await push(std::move(input));
      co_return;
    }
    // Preemptive check: the previous slice already exhausted the window budget.
    if (total_ >= args_.rate.inner) {
      if (args_.drop) {
        dropped_events_ += event_count(input);
        diagnostic::warning("dropping input due to rate limit")
          .primary(*args_.drop)
          .emit(ctx.dh());
        co_return;
      }
      stash(Pending{std::move(input), {}, 0, 0});
      co_return;
    }
    if (args_.drop) {
      // Find the first cutoff, if any, and drop everything after it.
      auto first_cutoff = physical_count(input);
      for (auto cutoff : find_cutoffs(input, ctx.dh())) {
        first_cutoff = cutoff;
        break;
      }
      if (first_cutoff != physical_count(input)) {
        auto kept = select_range(input, 0, first_cutoff);
        auto dropped = event_count(input) - event_count(kept);
        if (dropped > 0) {
          dropped_events_ += dropped;
          diagnostic::warning("dropping input due to rate limit")
            .primary(*args_.drop)
            .emit(ctx.dh());
          co_await push(std::move(kept));
        } else {
          co_await push(std::move(input));
        }
      } else {
        co_await push(std::move(input));
      }
      co_return;
    }
    // Partition the batch once. Pending ranges retain the original batch, so
    // every physical row is materialized at most once across all windows.
    auto ranges = std::vector<PhysicalRange>{};
    auto begin = size_t{0};
    for (auto cutoff : find_cutoffs(input, ctx.dh())) {
      ranges.push_back({begin, cutoff});
      begin = cutoff;
    }
    auto end = physical_count(input);
    if (ranges.empty()) {
      co_await push(std::move(input));
      co_return;
    }
    auto final_total = total_;
    if (begin == end) {
      // `find_cutoffs` resets `total_` after yielding, but an interval ending
      // exactly at the batch boundary still exhausts the current window.
      final_total = args_.rate.inner;
    } else if (event_count(input, begin, end) == 0) {
      // Keep an inactive-only physical suffix with the preceding interval.
      ranges.back().end = end;
      final_total = args_.rate.inner;
    } else {
      ranges.push_back({begin, end});
    }
    auto first = ranges.front();
    co_await push(select_range(input, first.begin, first.end));
    if (ranges.size() == 1) {
      total_ = final_total;
      co_return;
    }
    ranges.erase(ranges.begin());
    total_ = args_.rate.inner;
    stash(Pending{std::move(input), std::move(ranges), 0, final_total});
  }

  auto stash(Pending pending) -> void {
    TENZIR_ASSERT(start_);
    pending_ = std::move(pending);
    arm_timer(*start_ + args_.window.inner);
  }

  auto arm_timer(std::chrono::steady_clock::time_point deadline) -> void {
    auto sent = timer_sender_->try_send(deadline);
    TENZIR_ASSERT(sent.is_ok());
  }
  auto find_cutoffs(table_slice const& slice, diagnostic_handler& dh)
    -> generator<size_t> {
    const auto weights = eval(args_.weight, slice, dh);
    auto offset = size_t{};
    const auto is_cutoff = [&](const auto& weight) {
      if (not weight) {
        diagnostic::warning("expected `int`, got `null`")
          .primary(args_.weight)
          .note("treating as `0`")
          .emit(dh);
        return false;
      }
      if (*weight < 0) {
        diagnostic::warning("`weight` must not be negative")
          .primary(args_.weight)
          .note("treating as `0`")
          .emit(dh);
        return false;
      }
      auto sum = checked_add(total_, *weight);
      if (not sum) {
        diagnostic::warning("`weight` sum overflowed")
          .primary(args_.weight)
          .note("treating as hitting the rate limit")
          .emit(dh);
        total_ = args_.rate.inner;
        return true;
      }
      total_ = *sum;
      return total_ >= args_.rate.inner;
    };
    const auto emit_cutoffs = [&](auto values) -> generator<size_t> {
      for (const auto weight : values) {
        offset += 1;
        if (is_cutoff(weight)) {
          co_yield offset;
          total_ = 0;
        }
      }
    };
    for (const auto& part : weights) {
      if (auto ints = part.as<int64_type>()) {
        for (auto cutoff : emit_cutoffs(ints->values())) {
          co_yield cutoff;
        }
        continue;
      }
      if (auto uints = part.as<uint64_type>()) {
        for (auto cutoff : emit_cutoffs(uints->values())) {
          co_yield cutoff;
        }
        continue;
      }
      offset += part.length();
      diagnostic::warning("expected `int`, got `{}`", part.type.kind())
        .primary(args_.weight)
        .note("treating as `0`")
        .emit(dh);
    }
  }

  auto find_cutoffs(nova::Events const& events, diagnostic_handler& dh)
    -> generator<size_t> {
    TENZIR_ASSERT(evaluator_);
    auto weights = evaluator_->eval(events, nova::EvalCtx{dh});
    auto ints = weights.get_alternative<nova::Int>();
    auto uints = weights.get_alternative<nova::UInt>();
    for (auto i = nova::storage::Index{0}; i < events.length(); ++i) {
      if (not events.mask.get(i)) {
        continue;
      }
      auto weight = uint64_t{0};
      if (ints and ints->present.get(i)) {
        auto value = *ints->data.get(i);
        if (value < 0) {
          diagnostic::warning("`weight` must not be negative")
            .primary(args_.weight)
            .note("treating as `0`")
            .emit(dh);
          continue;
        }
        weight = static_cast<uint64_t>(value);
      } else if (uints and uints->present.get(i)) {
        weight = *uints->data.get(i);
      } else {
        diagnostic::warning("expected `int`")
          .primary(args_.weight)
          .note("treating as `0`")
          .emit(dh);
        continue;
      }
      auto sum = checked_add(total_, weight);
      if (not sum) {
        diagnostic::warning("`weight` sum overflowed")
          .primary(args_.weight)
          .note("treating as hitting the rate limit")
          .emit(dh);
        total_ = args_.rate.inner;
      } else {
        total_ = *sum;
      }
      if (total_ >= args_.rate.inner) {
        co_yield static_cast<size_t>(i + 1);
        total_ = 0;
      }
    }
  }

  static auto physical_count(table_slice const& input) -> size_t {
    return input.rows();
  }

  static auto physical_count(nova::Events const& input) -> size_t {
    return static_cast<size_t>(input.length());
  }

  static auto event_count(table_slice const& input) -> size_t {
    return input.rows();
  }

  static auto event_count(nova::Events const& input) -> size_t {
    return static_cast<size_t>(input.active_count());
  }

  static auto event_count(table_slice const& input, size_t begin, size_t end)
    -> size_t {
    TENZIR_UNUSED(input);
    return end - begin;
  }

  static auto event_count(nova::Events const& input, size_t begin, size_t end)
    -> size_t {
    auto result = size_t{0};
    for (auto i = begin; i < end; ++i) {
      result += input.mask.get(detail::narrow<nova::storage::Index>(i));
    }
    return result;
  }

  static auto select_range(table_slice input, size_t begin, size_t end)
    -> table_slice {
    return subslice(input, begin, end);
  }

  static auto select_range(nova::Events const& input, size_t begin, size_t end)
    -> nova::Events {
    return nova::subslice(input, detail::narrow<nova::storage::Index>(begin),
                          detail::narrow<nova::storage::Index>(end));
  }

  auto finalize(Push<Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    if (pending_) {
      auto pending = std::move(*pending_);
      pending_ = None{};
      if (pending.ranges.empty()) {
        co_await push(std::move(pending.input));
      } else {
        auto begin = pending.ranges[pending.next].begin;
        auto end = pending.ranges.back().end;
        co_await push(select_range(std::move(pending.input), begin, end));
      }
    }
    if (args_.drop and dropped_events_ > 0) {
      throttle_metrics_.emit({{"dropped_events", int64_t(dropped_events_)}});
    }
    co_return FinalizeBehavior::done;
  }

  ThrottleArgs args_;
  Option<std::chrono::steady_clock::time_point> start_;
  uint64_t total_ = 0;
  metric_handler throttle_metrics_ = {};
  uint64_t dropped_events_ = 0;
  Option<std::chrono::steady_clock::time_point> last_emit_ = None{};
  bool stopping_ = false;
  /// Input that exhausted the current window budget, released by the timer.
  Option<Pending> pending_ = None{};
  /// Wakes the pacing timer in `await_task()`.
  Option<Sender<std::chrono::steady_clock::time_point>> timer_sender_ = None{};
  std::shared_ptr<Receiver<std::chrono::steady_clock::time_point>>
    timer_receiver_;
  /// Interrupts an in-flight pacing sleep on `stop()`.
  folly::CancellationSource stop_cancel_;
  Option<nova::Evaluator> evaluator_;
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "throttle";
  }

  auto describe() const -> Description override {
    auto d
      = Describer<ThrottleArgs, Throttle<table_slice>, Throttle<nova::Events>>{};
    auto rate = d.named("rate", &ThrottleArgs::rate);
    auto window = d.named_optional("window", &ThrottleArgs::window);
    d.named_optional("weight", &ThrottleArgs::weight, "int");
    d.named("drop", &ThrottleArgs::drop);
    d.validate([rate, window](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(rate)) {
        if (value->inner == 0) {
          diagnostic::error("`rate` must be a positive value")
            .primary(*value)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(window)) {
        if (value->inner <= duration::zero()) {
          diagnostic::error("`window` must be a positive duration")
            .primary(*value)
            .emit(ctx);
        }
      }
      return {};
    });
    return d.invariant_order();
  }
};

} // namespace tenzir::plugins::throttle

TENZIR_REGISTER_PLUGIN(tenzir::plugins::throttle::plugin)
