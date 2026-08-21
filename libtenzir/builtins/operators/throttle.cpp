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

class throttle_operator final : public crtp_operator<throttle_operator> {
public:
  throttle_operator() = default;

  explicit throttle_operator(throttle_args args) : args_{std::move(args)} {
  }

  auto find_cutoffs(uint64_t& total, const table_slice& slice,
                    diagnostic_handler& dh) const -> generator<size_t> {
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
      auto sum = checked_add(total, *weight);
      if (not sum) {
        diagnostic::warning("`weight` sum overflowed")
          .primary(args_.weight)
          .note("treating as hitting the rate limit")
          .emit(dh);
        total = args_.rate.inner;
        return true;
      }
      total = *sum;
      return total >= args_.rate.inner;
    };
    for (const auto& part : weights) {
      if (auto ints = part.as<int64_type>()) {
        for (const auto weight : ints->values()) {
          offset += 1;
          if (is_cutoff(weight)) {
            co_yield offset;
            total = 0;
          }
        }
        continue;
      }
      if (auto uints = part.as<uint64_type>()) {
        for (const auto weight : uints->values()) {
          offset += 1;
          if (is_cutoff(weight)) {
            co_yield offset;
            total = 0;
          }
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

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto& dh = ctrl.diagnostics();
    auto start = std::chrono::steady_clock::now();
    auto last_emit = start;
    auto total = uint64_t{};
    auto dropped_events = uint64_t{};
    auto metrics = ctrl.metrics({
      "tenzir.metrics.throttle",
      record_type{
        {"dropped_events", int64_type{}},
      },
    });
    for (auto&& slice : input) {
      auto now = std::chrono::steady_clock::now();
      if (args_.drop and now - last_emit >= std::chrono::seconds{1}) {
        last_emit = now;
        if (dropped_events > 0) {
          metrics.emit({{"dropped_events", dropped_events}});
          dropped_events = 0;
        }
      }
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (now - start >= args_.window.inner) {
        start = now;
        total = 0;
      }
      if (total >= args_.rate.inner) {
        if (args_.drop) {
          dropped_events += slice.rows();
          diagnostic::warning("dropping input due to rate limit")
            .primary(*args_.drop)
            .emit(dh);
          co_yield {};
          continue;
        }
        auto delay = args_.window.inner - (now - start);
        ctrl.self().run_delayed_weak(delay, [&] {
          ctrl.set_waiting(false);
        });
        ctrl.set_waiting(true);
        co_yield {};
        now = std::chrono::steady_clock::now();
        start = now;
        total = 0;
      }
      if (args_.drop) {
        const auto cutoff = find_cutoffs(total, slice, dh).next();
        if (cutoff and cutoff.value() != slice.rows()) {
          dropped_events += slice.rows() - cutoff.value();
          diagnostic::warning("dropping input due to rate limit")
            .primary(*args_.drop)
            .emit(dh);
          co_yield subslice(slice, 0, cutoff.value());
        } else {
          co_yield std::move(slice);
        }
        continue;
      }
      auto begin = size_t{};
      for (auto cutoff : find_cutoffs(total, slice, dh)) {
        co_yield subslice(slice, begin, cutoff);
        begin = cutoff;
        auto delay = args_.window.inner - (now - start);
        ctrl.self().run_delayed_weak(delay, [&] {
          ctrl.set_waiting(false);
        });
        ctrl.set_waiting(true);
        co_yield {};
        now = std::chrono::steady_clock::now();
        start = now;
      }
      if (begin != slice.rows()) {
        co_yield subslice(slice, begin, slice.rows());
      }
    }
    if (args_.drop and dropped_events > 0) {
      metrics.emit({{"dropped_events", dropped_events}});
    }
  }

  auto name() const -> std::string override {
    return "throttle";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    return {filter, order, copy()};
  }

  friend auto inspect(auto& f, throttle_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  throttle_args args_;
};

struct ThrottleArgs {
  located<uint64_t> rate;
  located<duration> window{std::chrono::seconds{1}, location::unknown};
  ast::expression weight{ast::constant{uint64_t{1}, location::unknown}};
  Option<location> drop;
};

class Throttle final : public Operator<table_slice, table_slice> {
public:
  explicit Throttle(ThrottleArgs args) : args_{std::move(args)} {
    auto [sender, receiver] = channel<std::chrono::steady_clock::time_point>(1);
    timer_sender_ = std::move(sender);
    timer_receiver_
      = std::make_shared<Receiver<std::chrono::steady_clock::time_point>>(
        std::move(receiver));
  }

  auto start(OpCtx& ctx) -> Task<void> override {
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

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
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

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not result.try_as<TimerFired>() or not pending_) {
      co_return;
    }
    auto input = std::move(*pending_);
    pending_ = None{};
    start_ = std::chrono::steady_clock::now();
    total_ = 0;
    co_await emit(std::move(input), push, ctx);
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

  auto emit(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> {
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
        dropped_events_ += input.rows();
        diagnostic::warning("dropping input due to rate limit")
          .primary(*args_.drop)
          .emit(ctx.dh());
        co_return;
      }
      stash(std::move(input));
      co_return;
    }
    if (args_.drop) {
      // Find the first cutoff, if any, and drop everything after it.
      auto first_cutoff = input.rows();
      for (auto cutoff : find_cutoffs(input, ctx.dh())) {
        first_cutoff = cutoff;
        break;
      }
      if (first_cutoff != input.rows()) {
        dropped_events_ += input.rows() - first_cutoff;
        diagnostic::warning("dropping input due to rate limit")
          .primary(*args_.drop)
          .emit(ctx.dh());
        co_await push(subslice(input, 0, first_cutoff));
      } else {
        co_await push(std::move(input));
      }
      co_return;
    }
    // Wait path: push events up to the first cutoff and stash the remainder
    // until the window rolls over.
    auto begin = size_t{0};
    for (auto cutoff : find_cutoffs(input, ctx.dh())) {
      co_await push(subslice(input, begin, cutoff));
      begin = cutoff;
      // `find_cutoffs` reset `total_` on yield, but the budget of the current
      // window is spent either way.
      total_ = args_.rate.inner;
      if (begin != input.rows()) {
        stash(subslice(input, begin, input.rows()));
      }
      co_return;
    }
    if (begin != input.rows()) {
      co_await push(subslice(input, begin, input.rows()));
    }
  }

  auto stash(table_slice remainder) -> void {
    TENZIR_ASSERT(start_);
    pending_ = std::move(remainder);
    auto sent = timer_sender_->try_send(*start_ + args_.window.inner);
    TENZIR_ASSERT(sent.is_ok());
  }
  auto find_cutoffs(const table_slice& slice, diagnostic_handler& dh)
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

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    if (pending_) {
      auto input = std::move(*pending_);
      pending_ = None{};
      co_await push(std::move(input));
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
  Option<table_slice> pending_ = None{};
  /// Wakes the pacing timer in `await_task()`.
  Option<Sender<std::chrono::steady_clock::time_point>> timer_sender_ = None{};
  std::shared_ptr<Receiver<std::chrono::steady_clock::time_point>>
    timer_receiver_;
  /// Interrupts an in-flight pacing sleep on `stop()`.
  folly::CancellationSource stop_cancel_;
};

class plugin final : public virtual operator_plugin2<throttle_operator>,
                     public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = throttle_args{};
    TRY(argument_parser2::operator_("throttle")
          .named("rate", args.rate)
          .named_optional("window", args.window)
          .named_optional("weight", args.weight, "int")
          .named("drop", args.drop)
          .parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<throttle_operator>(std::move(args));
  }

  auto describe() const -> Description override {
    auto d = Describer<ThrottleArgs, Throttle>{};
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
