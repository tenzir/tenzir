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

class assert_throughput_operator final
  : public crtp_operator<assert_throughput_operator> {
public:
  assert_throughput_operator() = default;

  assert_throughput_operator(located<uint64_t> min_events,
                             located<duration> within,
                             Option<located<uint64_t>> max_events,
                             std::optional<located<uint64_t>> retries)
    : min_events_{min_events},
      within_{within},
      max_events_{max_events},
      retries_{retries} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto num_events = uint64_t{0};
    auto num_failed = uint64_t{0};
    auto check = [&] {
      auto max_exceeded = max_events_ and num_events > max_events_->inner;
      if (num_events >= min_events_.inner and not max_exceeded) {
        num_events = 0;
        num_failed = 0;
        return;
      }
      ++num_failed;
      diagnostic::warning(
        "assertion failure: {}{}",
        max_exceeded ? "exceeded maximum throughput requirement"
                     : "failed to meet minimum throughput requirement",
        num_failed > 1 ? fmt::format(" {} times", num_failed) : "")
        .note("observed {} events, expected {} {}", num_events,
              max_exceeded ? "at most" : "at least",
              max_exceeded ? max_events_->inner : min_events_.inner)
        .compose([&](diagnostic_builder dh) {
          if (not retries_) {
            return dh;
          }
          if (num_failed - 1 < retries_->inner) {
            return dh;
          }
          return std::move(dh)
            .severity(severity::error)
            .primary(*retries_, "exceeded number of retries");
        })
        .primary(max_exceeded ? *max_events_ : min_events_)
        .emit(ctrl.diagnostics());
    };
    detail::weak_run_delayed_loop(&ctrl.self(), within_.inner, std::move(check),
                                  false);
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      num_events += slice.rows();
      co_yield {};
    }
  }

  auto name() const -> std::string override {
    return "assert_throughput";
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter;
    return optimize_result::order_invariant(*this, order);
  }

  friend auto inspect(auto& f, assert_throughput_operator& x) -> bool {
    return f.object(x).fields(f.field("min_events", x.min_events_),
                              f.field("within", x.within_),
                              f.field("max_events", x.max_events_),
                              f.field("retries", x.retries_));
  }

private:
  located<uint64_t> min_events_ = {};
  located<duration> within_ = {};
  Option<located<uint64_t>> max_events_ = {};
  std::optional<located<uint64_t>> retries_ = {};
};

struct AssertThroughputArgs final {
  located<uint64_t> min_events;
  Option<located<uint64_t>> max_events;
  duration within;
  Option<uint64_t> retries;
};

class AssertThroughput final : public Operator<table_slice, table_slice> {
public:
  AssertThroughput(AssertThroughputArgs args) : args_{args} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    co_await Operator<table_slice, table_slice>::start(ctx);
    // Spawn a timer task with absolute scheduling to avoid cumulative drift.
    ctx.spawn_task([args = args_, events = num_events_,
                    &dh = ctx.dh()] -> Task<void> {
      auto num_failed = uint64_t{0};
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
          num_failed = 0;
          continue;
        }
        ++num_failed;
        auto sev = args.retries and num_failed > *args.retries
                     ? severity::error
                     : severity::warning;
        diagnostic::builder(
          sev, "{}{}",
          max_exceeded ? "exceeded maximum throughput requirement"
                       : "failed to meet minimum throughput requirement",
          num_failed > 1 ? fmt::format(" {} times", num_failed) : "")
          .note("observed {} events, expected {} {}", count,
                max_exceeded ? "at most" : "at least",
                max_exceeded ? args.max_events->inner : args.min_events.inner)
          .primary(max_exceeded ? *args.max_events : args.min_events)
          .emit(dh);
      }
    });
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx&)
    -> Task<void> override {
    num_events_->fetch_add(input.rows());
    co_await push(std::move(input));
  }

private:
  AssertThroughputArgs args_;
  std::shared_ptr<std::atomic<uint64_t>> num_events_
    = std::make_shared<std::atomic<uint64_t>>(0);
};

class plugin final
  : public virtual operator_plugin2<assert_throughput_operator>,
    public virtual OperatorPlugin {
public:
  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto min_events = located<uint64_t>{};
    auto within = located<duration>{};
    auto max_events = Option<located<uint64_t>>{};
    auto retries = std::optional<located<uint64_t>>{};
    auto parser = argument_parser2::operator_("assert_throughput");
    parser.positional("min_events", min_events);
    parser.named("max_events", max_events);
    parser.named("retries", retries);
    parser.named("within", within);
    TRY(parser.parse(inv, ctx));
    if (max_events and max_events->inner < min_events.inner) {
      diagnostic::error("`max_events` must not be less than `min_events`")
        .primary(*max_events)
        .secondary(min_events, "`min_events`")
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<assert_throughput_operator>(min_events, within,
                                                        max_events, retries);
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
