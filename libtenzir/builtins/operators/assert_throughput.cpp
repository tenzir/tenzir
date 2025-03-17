//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::assert_throughput {

namespace {

class assert_throughput_operator final
  : public crtp_operator<assert_throughput_operator> {
public:
  assert_throughput_operator() = default;

  assert_throughput_operator(located<uint64_t> min_events,
                             located<duration> within,
                             std::optional<located<uint64_t>> retries)
    : min_events_{min_events}, within_{within}, retries_{retries} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto num_events = size_t{0};
    auto num_failed = size_t{0};
    auto check = [&] {
      if (num_events >= min_events_.inner) {
        num_events = 0;
        num_failed = 0;
        return;
      }
      ++num_failed;
      diagnostic::warning(
        "assertion failure: failed to meet throughput requirements {}",
        num_failed == 1 ? "once" : fmt::format("{} times", num_failed))
        .note("at {:.2f}% of the expected throughput",
              num_events * 100.0 / min_events_.inner)
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
                              f.field("retries", x.retries_));
  }

private:
  located<uint64_t> min_events_ = {};
  located<duration> within_ = {};
  std::optional<located<uint64_t>> retries_ = {};
};

class plugin final
  : public virtual operator_plugin2<assert_throughput_operator> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto min_events = located<uint64_t>{};
    auto within = located<duration>{};
    auto retries = std::optional<located<uint64_t>>{};
    auto parser = argument_parser2::operator_("assert_throughput");
    parser.positional("min_events", min_events);
    parser.named("retries", retries);
    parser.named("within", within);
    TRY(parser.parse(inv, ctx));
    return std::make_unique<assert_throughput_operator>(min_events, within,
                                                        retries);
  }
};

} // namespace

} // namespace tenzir::plugins::assert_throughput

TENZIR_REGISTER_PLUGIN(tenzir::plugins::assert_throughput::plugin)
