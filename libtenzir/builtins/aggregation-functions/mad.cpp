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
#include <tenzir/nova/aggregation/statistics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <algorithm>
#include <cmath>
#include <limits>
#include <numeric>
#include <utility>
#include <vector>

namespace tenzir::plugins::mad {

namespace {

using WideInt = __int128_t;
using WideUint = __uint128_t;

/// Finds the middle value or values of `xs` in-place, reordering elements.
template <class T>
auto middle_values_inplace(std::vector<T>& xs) -> std::pair<T, T> {
  TENZIR_ASSERT(not xs.empty());
  auto const mid = xs.begin() + std::ssize(xs) / 2;
  std::nth_element(xs.begin(), mid, xs.end());
  if (xs.size() % 2 == 1) {
    return {*mid, *mid};
  }
  return {*std::max_element(xs.begin(), mid), *mid};
}

/// Computes the MAD of floating-point values in-place.
auto compute_floating_mad(std::vector<double> xs) -> double {
  auto const [lower, upper] = middle_values_inplace(xs);
  auto const median = std::midpoint(lower, upper);
  for (auto& x : xs) {
    x = std::abs(x - median);
  }
  auto const [lower_deviation, upper_deviation] = middle_values_inplace(xs);
  return std::midpoint(lower_deviation, upper_deviation);
}

struct IntegralMad {
  WideUint numerator;
  WideUint denominator;
};

/// Computes the MAD of integer values without narrowing before subtraction.
auto compute_integral_mad(std::vector<WideInt> xs) -> IntegralMad {
  auto const count = xs.size();
  auto const [lower, upper] = middle_values_inplace(xs);
  auto const doubled_median = lower + upper;
  auto deviations = std::vector<WideUint>{};
  deviations.reserve(count);
  for (auto const x : xs) {
    auto const centered = x * 2 - doubled_median;
    deviations.push_back(
      static_cast<WideUint>(centered < 0 ? -centered : centered));
  }
  auto const [lower_deviation, upper_deviation]
    = middle_values_inplace(deviations);
  if (count % 2 == 1) {
    return {upper_deviation, WideUint{2}};
  }
  return {lower_deviation + upper_deviation, WideUint{4}};
}

class mad_instance final : public aggregation_instance {
public:
  explicit mad_instance(ast::expression expr) : expr_{std::move(expr)} {
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
            if constexpr (std::same_as<Type, double_type>) {
              if (std::isnan(*value)) {
                continue;
              }
            }
            values_.emplace_back(*value);
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
              values_.emplace_back(*value);
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
    if (state_ == state::none or state_ == state::failed or values_.empty()) {
      return data{};
    }
    if (state_ == state::dur) {
      auto values = std::vector<WideInt>{};
      values.reserve(values_.size());
      for (auto const& value : values_) {
        auto const* x = try_as<duration>(&value);
        TENZIR_ASSERT(x);
        values.push_back(static_cast<WideInt>(x->count()));
      }
      auto const result = compute_integral_mad(std::move(values));
      // Compare the exact rational result before narrowing it to the integral
      // duration representation.
      constexpr auto limit
        = static_cast<WideUint>(std::numeric_limits<duration::rep>::max());
      if (result.numerator > limit * result.denominator) {
        return data{};
      }
      return duration{
        static_cast<duration::rep>(result.numerator / result.denominator)};
    }
    auto has_floating = false;
    for (auto const& value : values_) {
      if (is<double>(value)) {
        has_floating = true;
        break;
      }
    }
    if (has_floating) {
      auto values = std::vector<double>{};
      values.reserve(values_.size());
      for (auto const& value : values_) {
        if (auto const* x = try_as<int64_t>(&value)) {
          values.push_back(static_cast<double>(*x));
          continue;
        }
        if (auto const* x = try_as<uint64_t>(&value)) {
          values.push_back(static_cast<double>(*x));
          continue;
        }
        auto const* x = try_as<double>(&value);
        TENZIR_ASSERT(x);
        values.push_back(*x);
      }
      return compute_floating_mad(std::move(values));
    }
    auto values = std::vector<WideInt>{};
    values.reserve(values_.size());
    for (auto const& value : values_) {
      if (auto const* x = try_as<int64_t>(&value)) {
        values.push_back(static_cast<WideInt>(*x));
        continue;
      }
      auto const* x = try_as<uint64_t>(&value);
      TENZIR_ASSERT(x);
      values.push_back(static_cast<WideInt>(*x));
    }
    auto const result = compute_integral_mad(std::move(values));
    return static_cast<double>(result.numerator)
           / static_cast<double>(result.denominator);
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto const fb_state = [&] {
      switch (state_) {
        case state::none:
          return fbs::aggregation::MadState::None;
        case state::failed:
          return fbs::aggregation::MadState::Failed;
        case state::dur:
          return fbs::aggregation::MadState::Duration;
        case state::numeric:
          return fbs::aggregation::MadState::Numeric;
      }
      TENZIR_UNREACHABLE();
    }();
    auto offsets = std::vector<flatbuffers::Offset<fbs::Data>>{};
    offsets.reserve(values_.size());
    for (auto const& value : values_) {
      offsets.push_back(pack(fbb, value));
    }
    auto const fb_values = fbb.CreateVector(offsets);
    auto const fb_mad = fbs::aggregation::CreateMad(fbb, fb_values, fb_state);
    fbb.Finish(fb_mad);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    auto const fb = flatbuffer<fbs::aggregation::Mad>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN(
        "failed to restore `mad` aggregation instance: invalid FlatBuffer");
      return false;
    }
    auto const restored_state = [&]() -> Option<state> {
      switch ((*fb)->state()) {
        case fbs::aggregation::MadState::None:
          return state::none;
        case fbs::aggregation::MadState::Failed:
          return state::failed;
        case fbs::aggregation::MadState::Duration:
          return state::dur;
        case fbs::aggregation::MadState::Numeric:
          return state::numeric;
      }
      return None{};
    }();
    if (not restored_state) {
      TENZIR_WARN(
        "failed to restore `mad` aggregation instance: unknown state value");
      return false;
    }
    auto const* values = (*fb)->values();
    if (not values) {
      TENZIR_WARN(
        "failed to restore `mad` aggregation instance: missing values");
      return false;
    }
    values_.clear();
    values_.reserve(values->size());
    for (auto const* fb_value : *values) {
      if (not fb_value) {
        TENZIR_WARN(
          "failed to restore `mad` aggregation instance: missing value");
        return false;
      }
      auto value = data{};
      if (auto err = unpack(*fb_value, value); err.valid()) {
        TENZIR_WARN("failed to restore `mad` aggregation instance: {}", err);
        return false;
      }
      auto const is_numeric
        = is<int64_t>(value) or is<uint64_t>(value) or is<double>(value);
      if ((*restored_state == state::numeric and not is_numeric)
          or (*restored_state == state::dur and not is<duration>(value))) {
        TENZIR_WARN(
          "failed to restore `mad` aggregation instance: invalid value type");
        return false;
      }
      values_.push_back(std::move(value));
    }
    state_ = *restored_state;
    return true;
  }

  auto reset() -> void override {
    values_.clear();
    state_ = state::none;
  }

private:
  ast::expression expr_;
  std::vector<data> values_;
  enum class state { none, failed, dur, numeric } state_{state::none};
};

struct MadArgs {
  nova::ValueArgument x;
};

/// The buffered values of one `mad`, shared by the accumulator and the list
/// kernel. Integers and durations are kept exact; any `float` makes the
/// computation floating-point, as in the legacy implementation.
class MadValues {
public:
  template <class T>
  auto add(T value) -> void {
    if constexpr (std::same_as<T, nova::Float>) {
      floats_.push_back(value);
    } else if constexpr (std::same_as<T, nova::Duration>) {
      integers_.push_back(static_cast<WideInt>(value.count()));
    } else {
      integers_.push_back(static_cast<WideInt>(value));
    }
  }

  auto get(nova_statistics::NumericKind const& kind) const -> nova::Data {
    using enum nova_statistics::NumericKind::Kind;
    if (kind.kind() == none or kind.kind() == failed
        or (integers_.empty() and floats_.empty())) {
      return nova::Data{};
    }
    if (kind.kind() == duration) {
      auto const result = compute_integral_mad(integers_);
      // Compare the exact rational result before narrowing it to the integral
      // duration representation.
      constexpr auto limit = static_cast<WideUint>(
        std::numeric_limits<nova::Duration::rep>::max());
      if (result.numerator > limit * result.denominator) {
        return nova::Data{};
      }
      return nova::Data{nova::Duration{static_cast<nova::Duration::rep>(
        result.numerator / result.denominator)}};
    }
    if (not floats_.empty()) {
      auto values = floats_;
      values.reserve(floats_.size() + integers_.size());
      for (auto const x : integers_) {
        values.push_back(static_cast<double>(x));
      }
      return nova::Data{compute_floating_mad(std::move(values))};
    }
    auto const result = compute_integral_mad(integers_);
    return nova::Data{static_cast<double>(result.numerator)
                      / static_cast<double>(result.denominator)};
  }

private:
  std::vector<WideInt> integers_;
  std::vector<double> floats_;
};

class MadFunction final {
public:
  static auto eval(MadArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    return nova_statistics::eval_statistic<MadValues>(args.x, frame, true, [] {
      return MadValues{};
    });
  }

  auto update(MadArgs const& args, nova::EvalFrame frame) -> void {
    kind_.visit(args.x.data, frame.mask(), args.x.source, frame,
                [&](auto value) {
                  values_.add(value);
                });
  }

  auto get() const -> nova::Data {
    return values_.get(kind_);
  }

  auto reset() -> void {
    kind_ = nova_statistics::NumericKind{};
    values_ = {};
  }

private:
  nova_statistics::NumericKind kind_;
  MadValues values_;
};

class plugin final : public aggregation_plugin, public nova::AggregationPlugin {
public:
  auto name() const -> std::string override {
    return "mad";
  }

  auto describe() const -> nova::AggregationDescription override {
    auto d = nova::AggregationDescriber<MadArgs, MadFunction>{};
    d.positional("x", &MadArgs::x, "number|duration");
    return std::move(d).finish();
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto list_call_result_type(type const& input_type) const
    -> Option<type> override {
    if (is<duration_type>(input_type)) {
      return type{duration_type{}};
    }
    if (input_type.kind().is_any<double_type, int64_type, uint64_type>()) {
      return type{double_type{}};
    }
    return None{};
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number|duration")
          .parse(inv, ctx));
    return std::make_unique<mad_instance>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::mad

TENZIR_REGISTER_PLUGIN(tenzir::plugins::mad::plugin)
