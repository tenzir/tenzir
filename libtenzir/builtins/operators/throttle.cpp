//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/weak_run_delayed.hpp"

#include <tenzir/checked_math.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view.hpp>

#include <arrow/type.h>

#include <chrono>

namespace tenzir::plugins::throttle {

struct throttle_args final {
  located<uint64_t> rate;
  located<duration> window{std::chrono::seconds{1}, location::unknown};
  ast::expression weight{ast::constant{uint64_t{1}, location::unknown}};
  std::optional<location> drop;

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

class plugin final : public operator_plugin2<throttle_operator> {
public:
  auto make(invocation inv, session ctx) const
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
};

} // namespace tenzir::plugins::throttle

TENZIR_REGISTER_PLUGIN(tenzir::plugins::throttle::plugin)
