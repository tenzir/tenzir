//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <cmath>
#include <numbers>
#include <tuple>

namespace tenzir::plugins::ewm {

namespace {

enum class ewm_stat { mean, variance, stddev };

template <ewm_stat Stat>
class ewm_function final : public function_plugin {
public:
  auto name() const -> std::string override {
    if constexpr (Stat == ewm_stat::mean) {
      return "ewma";
    } else if constexpr (Stat == ewm_stat::variance) {
      return "ewm_variance";
    } else {
      return "ewm_stddev";
    }
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto alpha_arg = Option<located<data>>{};
    auto span_arg = Option<located<data>>{};
    auto com_arg = Option<located<data>>{};
    auto halflife_arg = Option<located<data>>{};
    auto times_expr = Option<ast::expression>{};
    auto adjust_arg = Option<located<bool>>{};
    auto bias_arg = Option<located<bool>>{};
    auto ignore_nulls_arg = Option<located<bool>>{};
    auto parser = argument_parser2::function(name());
    parser.positional("xs", expr, "list")
      .named("alpha", alpha_arg, "number")
      .named("span", span_arg, "number")
      .named("com", com_arg, "number")
      .named("halflife", halflife_arg, "number or duration")
      .named("times", times_expr, "list")
      .named("adjust", adjust_arg);
    if constexpr (Stat != ewm_stat::mean) {
      parser.named("bias", bias_arg);
    }
    parser.named("ignore_nulls", ignore_nulls_arg);
    TRY(parser.parse(inv, ctx));
    auto const decay_args = (alpha_arg ? 1 : 0) + (span_arg ? 1 : 0)
                            + (com_arg ? 1 : 0) + (halflife_arg ? 1 : 0);
    if (decay_args != 1) {
      diagnostic::error("expected exactly one of `alpha`, `span`, `com`, or "
                        "`halflife`")
        .primary(inv.call)
        .emit(ctx);
      return failure::promise();
    }
    auto const to_number = [&](located<data> const& arg,
                               std::string_view option) -> failure_or<double> {
      return match(
        arg.inner,
        [](double const& x) -> failure_or<double> {
          return x;
        },
        [](int64_t const& x) -> failure_or<double> {
          return static_cast<double>(x);
        },
        [](uint64_t const& x) -> failure_or<double> {
          return static_cast<double>(x);
        },
        [&](auto const&) -> failure_or<double> {
          diagnostic::error("expected `number` for `{}`", option)
            .primary(arg)
            .emit(ctx);
          return failure::promise();
        });
    };
    // The decay comes from exactly one of `alpha`, `span`, `com`, or
    // `halflife`. A duration `halflife` selects time-aware decay and requires
    // `times`; all other specifications count observations.
    auto numeric_halflife = Option<double>{};
    auto duration_halflife = Option<duration>{};
    if (halflife_arg) {
      if (auto const* value = try_as<duration>(halflife_arg->inner)) {
        duration_halflife = *value;
      } else {
        TRY(auto numeric_value, to_number(*halflife_arg, "halflife"));
        numeric_halflife = numeric_value;
      }
    }
    if (duration_halflife and not times_expr) {
      diagnostic::error("`halflife` with a duration requires `times`")
        .primary(*halflife_arg)
        .emit(ctx);
      return failure::promise();
    }
    if (times_expr and not duration_halflife) {
      diagnostic::error("`times` requires a duration `halflife`")
        .primary(*times_expr)
        .emit(ctx);
      return failure::promise();
    }
    if (times_expr and adjust_arg and not adjust_arg->inner) {
      diagnostic::error("`adjust=false` is incompatible with `times`")
        .primary(*adjust_arg)
        .emit(ctx);
      return failure::promise();
    }
    auto alpha = double{};
    if (alpha_arg) {
      TRY(auto value, to_number(*alpha_arg, "alpha"));
      if (not std::isfinite(value) or not(value > 0.0) or value > 1.0) {
        diagnostic::error("expected `alpha` to be finite and in (0.0, 1.0]")
          .primary(*alpha_arg)
          .emit(ctx);
        return failure::promise();
      }
      alpha = value;
    } else if (span_arg) {
      TRY(auto value, to_number(*span_arg, "span"));
      if (not std::isfinite(value) or not(value >= 1.0)) {
        diagnostic::error("expected `span` to be finite and at least 1")
          .primary(*span_arg)
          .emit(ctx);
        return failure::promise();
      }
      alpha = 2.0 / (value + 1.0);
    } else if (com_arg) {
      TRY(auto value, to_number(*com_arg, "com"));
      if (not std::isfinite(value) or not(value >= 0.0)) {
        diagnostic::error("expected `com` to be finite and non-negative")
          .primary(*com_arg)
          .emit(ctx);
        return failure::promise();
      }
      alpha = 1.0 / (1.0 + value);
    } else if (numeric_halflife) {
      if (not std::isfinite(*numeric_halflife)
          or not(*numeric_halflife > 0.0)) {
        diagnostic::error("expected `halflife` to be finite and positive")
          .primary(*halflife_arg)
          .emit(ctx);
        return failure::promise();
      }
      alpha = -std::expm1(-std::numbers::ln2 / *numeric_halflife);
    }
    auto const halflife_ns
      = duration_halflife
          ? std::chrono::duration_cast<std::chrono::nanoseconds>(
              *duration_halflife)
              .count()
          : int64_t{0};
    if (duration_halflife and halflife_ns <= 0) {
      diagnostic::error("expected `halflife` to be positive")
        .primary(*halflife_arg)
        .emit(ctx);
      return failure::promise();
    }
    auto const adjust = adjust_arg ? adjust_arg->inner : true;
    auto const bias = bias_arg ? bias_arg->inner : false;
    auto const ignore_nulls
      = ignore_nulls_arg ? ignore_nulls_arg->inner : false;
    return function_use::make([expr = std::move(expr),
                               times_expr = std::move(times_expr), alpha,
                               adjust, bias, ignore_nulls, halflife_ns](
                                evaluator eval, session ctx) -> multi_series {
      if constexpr (Stat == ewm_stat::mean) {
        // Only the variance and standard deviation read `bias`; the mean
        // instantiation captures it solely to keep the lambda uniform.
        std::ignore = bias;
      }
      // Extract a list element as a double, or null.
      auto const element
        = [](arrow::Array const& values, type_kind const value_kind,
             int64_t index) -> Option<double> {
        if (values.IsNull(index)) {
          return None{};
        }
        if (value_kind.is<int64_type>()) {
          return static_cast<double>(
            static_cast<arrow::Int64Array const&>(values).Value(index));
        }
        if (value_kind.is<uint64_type>()) {
          return static_cast<double>(
            static_cast<arrow::UInt64Array const&>(values).Value(index));
        }
        if (value_kind.is<double_type>()) {
          auto const value
            = static_cast<arrow::DoubleArray const&>(values).Value(index);
          // Like pandas, treat a NaN element as a missing observation.
          if (std::isnan(value)) {
            return None{};
          }
          return value;
        }
        return None{};
      };
      auto const process
        = [&](series const& subject, Option<series const&> times) -> series {
        if (is<null_type>(subject.type)) {
          return series::null(list_type{double_type{}}, subject.length());
        }
        auto const lists = subject.as<list_type>();
        if (not lists) {
          diagnostic::warning("expected `list`, but got `{}`",
                              subject.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(null_type{}, subject.length());
        }
        auto const value_type = lists->type.value_type();
        auto const value_kind = value_type.kind();
        if (not value_kind
                  .is_any<null_type, int64_type, uint64_type, double_type>()) {
          diagnostic::warning("expected a list of numbers, but got a list of "
                              "`{}`",
                              value_kind)
            .primary(expr)
            .emit(ctx);
          return series::null(null_type{}, subject.length());
        }
        auto times_array = Option<arrow::ListArray const&>{};
        auto times_values = Option<arrow::TimestampArray const&>{};
        if (times) {
          auto const times_lists = times->as<list_type>();
          if (not times_lists) {
            diagnostic::warning("expected `times` to be a list of times, but "
                                "got `{}`",
                                times->type.kind())
              .primary(*times_expr)
              .emit(ctx);
            return series::null(null_type{}, subject.length());
          }
          auto const times_value_kind = times_lists->type.value_type().kind();
          if (not times_value_kind.is<time_type>()) {
            diagnostic::warning("expected `times` to be a list of times, but "
                                "got a list of `{}`",
                                times_value_kind)
              .primary(*times_expr)
              .emit(ctx);
            return series::null(null_type{}, subject.length());
          }
          times_array = *times_lists->array;
          times_values
            = static_cast<arrow::TimestampArray const&>(*times_array->values());
        }
        auto const& array = *lists->array;
        auto const& values = *array.values();
        auto const num_rows = array.length();
        auto const result_type = list_type{double_type{}};
        auto const builder
          = result_type.make_arrow_builder(arrow_memory_pool());
        auto* value_builder
          = static_cast<arrow::DoubleBuilder*>(builder->value_builder());
        for (auto row = int64_t{0}; row < num_rows; ++row) {
          if (array.IsNull(row)) {
            check(builder->AppendNull());
            continue;
          }
          auto const row_begin = array.value_offset(row);
          auto const row_length = array.value_length(row);
          // Validate the time axis for this row before emitting anything.
          auto times_begin = int64_t{0};
          if (times_array) {
            auto valid = not times_array->IsNull(row)
                         and times_array->value_length(row) == row_length;
            if (not valid) {
              diagnostic::warning(
                "expected `times` to match the length of `xs`")
                .primary(*times_expr)
                .emit(ctx);
              check(builder->AppendNull());
              continue;
            }
            times_begin = times_array->value_offset(row);
            for (auto i = int64_t{0}; i < row_length; ++i) {
              auto const index = times_begin + i;
              if (times_values->IsNull(index)
                  or (i > 0
                      and times_values->Value(index)
                            < times_values->Value(index - 1))) {
                valid = false;
                break;
              }
            }
            if (not valid) {
              diagnostic::warning(
                "expected `times` to be non-decreasing without nulls")
                .primary(*times_expr)
                .emit(ctx);
              check(builder->AppendNull());
              continue;
            }
          }
          check(builder->Append());
          // The pandas-style update: track the weight of the accumulated
          // statistics (`old_wt`) relative to a new observation (`new_wt`),
          // where skipped periods decay the accumulated weight. The variance
          // follows a stable weighted-delta recurrence: `moment` carries the
          // biased weighted variance, while `sum_wt`/`sum_wt2` carry the sums
          // of weights and squared weights for the debiasing factor
          // `sum_wt^2 / (sum_wt^2 - sum_wt2)`. `sum_wt_cross` tracks the
          // denominator without subtraction for weights whose contribution
          // would otherwise be lost to rounding. All sums decay uniformly at
          // null positions, which leaves the debiasing factor unchanged, so
          // deferring the decay to the next observation via `gap` emits the
          // same values as the eager pandas loop.
          auto const old_wt_factor = 1.0 - alpha;
          auto const new_wt = adjust ? 1.0 : alpha;
          auto old_wt = 1.0;
          auto mean = double{};
          auto moment = 0.0;
          auto sum_wt = 1.0;
          auto sum_wt2 = 1.0;
          auto sum_wt_cross = 0.0;
          auto initialized = false;
          auto gap = double{0.0};
          auto last_time = int64_t{0};
          auto const emit = [&] {
            if constexpr (Stat == ewm_stat::mean) {
              check(value_builder->Append(mean));
            } else {
              auto value = Option<double>{};
              if (bias) {
                value = moment;
              } else {
                auto const numerator = sum_wt * sum_wt;
                auto const denominator = numerator - sum_wt2;
                if (denominator > 0.0) {
                  value = numerator / denominator * moment;
                } else if (sum_wt_cross > 0.0) {
                  // Divide the tiny moment before applying the weight sum so
                  // the correction factor itself does not overflow.
                  value = moment / sum_wt_cross * numerator;
                }
              }
              if (not value) {
                check(value_builder->AppendNull());
                return;
              }
              if constexpr (Stat == ewm_stat::stddev) {
                // Like the pandas `zsqrt` helper, clamp small negative
                // variances from floating-point error to zero.
                *value = *value < 0.0 ? 0.0 : std::sqrt(*value);
              }
              check(value_builder->Append(*value));
            }
          };
          for (auto i = int64_t{0}; i < row_length; ++i) {
            auto const x = value_kind.is<null_type>()
                             ? Option<double>{None{}}
                             : element(values, value_kind, row_begin + i);
            if (times_values) {
              auto const t = times_values->Value(times_begin + i);
              if (initialized and (x or not ignore_nulls)) {
                // Unsigned subtraction represents the full non-negative
                // distance without overflowing signed timestamp integers.
                auto const elapsed
                  = static_cast<uint64_t>(t) - static_cast<uint64_t>(last_time);
                gap += static_cast<double>(elapsed)
                       / static_cast<double>(halflife_ns);
              }
              last_time = t;
            }
            if (not x) {
              if (not ignore_nulls and initialized and not times_values) {
                gap += 1.0;
              }
              if (initialized) {
                emit();
              } else {
                check(value_builder->AppendNull());
              }
              continue;
            }
            if (not initialized) {
              initialized = true;
              mean = *x;
              emit();
              continue;
            }
            auto const decay = times_values
                                 ? std::exp2(-gap)
                                 : std::pow(old_wt_factor, gap + 1.0);
            gap = 0.0;
            old_wt *= decay;
            if (old_wt == 0.0 or std::isnan(mean)) {
              // Reset all state when history can no longer contribute: either
              // its weight has no representable value, or opposing infinities
              // collapsed the mean into NaN. Like pandas, restart all moments
              // from the new sample.
              mean = *x;
              moment = 0.0;
              sum_wt = 1.0;
              sum_wt2 = 1.0;
              sum_wt_cross = 0.0;
              old_wt = 1.0;
              emit();
              continue;
            }
            sum_wt *= decay;
            sum_wt2 *= decay * decay;
            sum_wt_cross *= decay * decay;
            auto const total_wt = old_wt + new_wt;
            auto const old_mean = mean;
            auto const numerator = old_wt * mean + new_wt * *x;
            mean = std::isfinite(mean) and std::isfinite(*x)
                       and not std::isfinite(numerator)
                     ? std::lerp(mean, *x, new_wt / total_wt)
                     : numerator / total_wt;
            if constexpr (Stat != ewm_stat::mean) {
              auto const old_fraction = old_wt / total_wt;
              auto const new_fraction = new_wt / total_wt;
              auto const scale = std::sqrt(old_fraction * new_fraction);
              auto const delta = old_mean - *x;
              auto const scaled_delta = std::isfinite(delta)
                                          ? scale * delta
                                          : scale * old_mean - scale * *x;
              moment = old_fraction * moment + scaled_delta * scaled_delta;
            }
            sum_wt_cross += 2.0 * sum_wt * new_wt;
            sum_wt += new_wt;
            sum_wt2 += new_wt * new_wt;
            old_wt = adjust ? total_wt : 1.0;
            if (not adjust) {
              sum_wt /= total_wt;
              sum_wt2 /= total_wt * total_wt;
              sum_wt_cross /= total_wt * total_wt;
            }
            emit();
          }
        }
        return series{type{result_type}, finish(*builder)};
      };
      if (times_expr) {
        return map_series(eval(expr), eval(*times_expr),
                          [&](series subject, series times) -> multi_series {
                            return process(subject, times);
                          });
      }
      return map_series(eval(expr), [&](series subject) -> multi_series {
        return process(subject, None{});
      });
    });
  }
};

using ewma = ewm_function<ewm_stat::mean>;
using ewm_variance = ewm_function<ewm_stat::variance>;
using ewm_stddev = ewm_function<ewm_stat::stddev>;

} // namespace

} // namespace tenzir::plugins::ewm

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ewm::ewma)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ewm::ewm_variance)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ewm::ewm_stddev)
