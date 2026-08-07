//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <algorithm>
#include <cmath>
#include <vector>

namespace tenzir::plugins::skewness {

namespace {

enum class method {
  moment,
  bowley,
};

/// Computes the type-7 (linear interpolation) quantile of sorted `xs`.
auto quantile_sorted(std::vector<double> const& xs, double q) -> double {
  TENZIR_ASSERT(not xs.empty());
  auto const h = static_cast<double>(xs.size() - 1) * q;
  auto const lo = static_cast<size_t>(h);
  if (lo + 1 == xs.size()) {
    return xs.back();
  }
  // std::lerp interpolates without overflowing for mixed-sign samples near
  // the float64 limit, where `xs[lo + 1] - xs[lo]` would be infinite.
  return std::lerp(xs[lo], xs[lo + 1], h - static_cast<double>(lo));
}

class skewness_instance final : public aggregation_instance {
public:
  skewness_instance(ast::expression expr, method m)
    : expr_{std::move(expr)}, method_{m} {
  }

  void update(table_slice const& input, session ctx) override {
    if (state_ == state::failed) {
      return;
    }
    for (auto& arg : eval(expr_, input, ctx)) {
      auto const f = detail::overload{
        [&]<concepts::one_of<double_type, int64_type, uint64_type> Type>(
          Type const&) {
          if (state_ != state::numeric and state_ != state::none) {
            diagnostic::warning("got incompatible types `duration` and `{}`",
                                arg.type.kind())
              .primary(expr_)
              .emit(ctx);
            state_ = state::failed;
            return;
          }
          state_ = state::numeric;
          auto const& array = as<type_to_arrow_array_t<Type>>(*arg.array);
          for (auto value : values3(array)) {
            if (not value) {
              continue;
            }
            auto const x = static_cast<double>(*value);
            if (std::isnan(x)) {
              continue;
            }
            add(x);
          }
        },
        [&](duration_type const&) {
          if (state_ != state::dur and state_ != state::none) {
            diagnostic::warning("got incompatible types `number` and `{}`",
                                arg.type.kind())
              .primary(expr_)
              .emit(ctx);
            state_ = state::failed;
            return;
          }
          state_ = state::dur;
          auto const& array = as<arrow::DurationArray>(*arg.array);
          for (auto value : values3(array)) {
            if (value) {
              add(static_cast<double>(value->count()));
            }
          }
        },
        [&](null_type const&) {
          // Silently ignore nulls, like the other statistics functions.
        },
        [&](auto const&) {
          diagnostic::warning("expected `int`, `uint`, `double` or "
                              "`duration`, got `{}`",
                              arg.type.kind())
            .primary(expr_)
            .emit(ctx);
          state_ = state::failed;
        },
      };
      match(arg.type, f);
    }
  }

  auto get() const -> data override {
    if (state_ == state::none or state_ == state::failed) {
      return data{};
    }
    switch (method_) {
      case method::moment:
        return get_moment();
      case method::bowley:
        return get_bowley();
    }
    TENZIR_UNREACHABLE();
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto const fb_state = [&] {
      switch (state_) {
        case state::none:
          return fbs::aggregation::SkewnessState::None;
        case state::failed:
          return fbs::aggregation::SkewnessState::Failed;
        case state::dur:
          return fbs::aggregation::SkewnessState::Duration;
        case state::numeric:
          return fbs::aggregation::SkewnessState::Numeric;
      }
      TENZIR_UNREACHABLE();
    }();
    auto const fb_values = fbb.CreateVector(values_);
    auto const fb_skewness = fbs::aggregation::CreateSkewness(
      fbb, count_, mean_, m2_, m3_, fb_values, fb_state);
    fbb.Finish(fb_skewness);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    auto const fb
      = flatbuffer<fbs::aggregation::Skewness>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN("failed to restore `skewness` aggregation instance: "
                  "invalid FlatBuffer");
      return false;
    }
    auto const* values = (*fb)->values();
    if (not values) {
      TENZIR_WARN(
        "failed to restore `skewness` aggregation instance: missing values");
      return false;
    }
    count_ = (*fb)->count();
    mean_ = (*fb)->mean();
    m2_ = (*fb)->m2();
    m3_ = (*fb)->m3();
    values_.assign(values->begin(), values->end());
    switch ((*fb)->state()) {
      case fbs::aggregation::SkewnessState::None:
        state_ = state::none;
        return true;
      case fbs::aggregation::SkewnessState::Failed:
        state_ = state::failed;
        return true;
      case fbs::aggregation::SkewnessState::Duration:
        state_ = state::dur;
        return true;
      case fbs::aggregation::SkewnessState::Numeric:
        state_ = state::numeric;
        return true;
    }
    TENZIR_WARN("failed to restore `skewness` aggregation instance: unknown "
                "state value");
    return false;
  }

  auto reset() -> void override {
    count_ = {};
    mean_ = {};
    m2_ = {};
    m3_ = {};
    values_.clear();
    state_ = state::none;
  }

private:
  auto add(double x) -> void {
    switch (method_) {
      case method::moment: {
        // Welford-style streaming update of the second and third central
        // moments; numerically stable, unlike accumulating E[x²] and E[x³].
        count_ += 1;
        auto const n = static_cast<double>(count_);
        auto const delta = x - mean_;
        auto const delta_n = delta / n;
        auto const term1 = delta * delta_n * (n - 1);
        mean_ += delta_n;
        m3_ += term1 * delta_n * (n - 2) - 3 * delta_n * m2_;
        m2_ += term1;
        return;
      }
      case method::bowley:
        values_.push_back(x);
        return;
    }
    TENZIR_UNREACHABLE();
  }

  auto get_moment() const -> data {
    if (count_ == 0) {
      return data{};
    }
    // Zero dispersion means no asymmetry. Returning 0.0 instead of the 0/0
    // NaN keeps perfectly regular inputs (e.g. beacon intervals) inside
    // predicates like `abs(skew) <= threshold`.
    if (m2_ <= 0.0) {
      return 0.0;
    }
    auto const n = static_cast<double>(count_);
    return std::sqrt(n) * m3_ / std::pow(m2_, 1.5);
  }

  auto get_bowley() const -> data {
    if (values_.empty()) {
      return data{};
    }
    auto sorted = values_;
    std::sort(sorted.begin(), sorted.end());
    auto const q1 = quantile_sorted(sorted, 0.25);
    auto const q2 = quantile_sorted(sorted, 0.5);
    auto const q3 = quantile_sorted(sorted, 0.75);
    if (not std::isfinite(q1) or not std::isfinite(q2)
        or not std::isfinite(q3)) {
      return data{};
    }
    // A degenerate interquartile range means zero dispersion, so there is no
    // asymmetry; see the note in `get_moment`.
    if (q3 == q1) {
      return 0.0;
    }
    auto skew_from_gaps = [](double upper, double lower) {
      auto const scale = std::max(upper, lower);
      TENZIR_ASSERT(scale > 0.0);
      upper /= scale;
      lower /= scale;
      return (upper - lower) / (upper + lower);
    };
    // Preserve close quartile gaps when direct subtraction is finite. Scaling
    // the gaps still keeps their sum from overflowing.
    auto const upper = q3 - q2;
    auto const lower = q2 - q1;
    if (std::isfinite(upper) and std::isfinite(lower)) {
      return skew_from_gaps(upper, lower);
    }
    // If a direct gap overflows, use scale invariance to bring the quartiles
    // into range before subtracting them.
    auto const scale = std::max({std::abs(q1), std::abs(q2), std::abs(q3)});
    return skew_from_gaps(q3 / scale - q2 / scale, q2 / scale - q1 / scale);
  }

  ast::expression expr_;
  method method_ = method::moment;
  // Streaming state for `method::moment`.
  size_t count_ = {};
  double mean_ = {};
  double m2_ = {};
  double m3_ = {};
  // Buffered values for `method::bowley`.
  std::vector<double> values_;
  enum class state { none, failed, dur, numeric } state_{state::none};
};

class plugin final : public aggregation_plugin {
public:
  auto name() const -> std::string override {
    return "skewness";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto list_call_result_type(type const& input_type) const
    -> Option<type> override {
    if (input_type.kind()
          .is_any<double_type, int64_type, uint64_type, duration_type>()) {
      return type{double_type{}};
    }
    return None{};
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    auto method_opt = std::optional<located<std::string>>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number|duration")
          .named("method", method_opt)
          .parse(inv, ctx));
    auto m = method::moment;
    if (method_opt) {
      if (method_opt->inner == "bowley") {
        m = method::bowley;
      } else if (method_opt->inner != "moment") {
        diagnostic::error("expected `method` to be `moment` or `bowley`, got "
                          "`{}`",
                          method_opt->inner)
          .primary(*method_opt)
          .emit(ctx);
        return failure::promise();
      }
    }
    return std::make_unique<skewness_instance>(std::move(expr), m);
  }
};

} // namespace

} // namespace tenzir::plugins::skewness

TENZIR_REGISTER_PLUGIN(tenzir::plugins::skewness::plugin)
