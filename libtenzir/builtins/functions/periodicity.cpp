//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/detail/periodicity.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>

namespace tenzir::plugins::periodicity {

namespace {

enum class extraction { ok, has_null, non_finite, wrong_type };

/// Beyond this many bins the FFT's memory and runtime become too expensive;
/// the user must pick a coarser resolution.
constexpr auto max_bins = int64_t{1} << 20;

/// Extracts the elements of row `row` of `array` as doubles. Numeric and
/// duration elements are accepted. Integral samples are shifted by their first
/// value before conversion so that doubles preserve variations above 2^53.
auto extract_doubles(arrow::ListArray const& array, int64_t row,
                     std::vector<double>& out) -> extraction {
  out.clear();
  auto const begin = array.value_offset(row);
  auto const length = array.value_length(row);
  out.reserve(length);
  return match(
    *array.values(),
    [&](arrow::NullArray const&) {
      return length > 0 ? extraction::has_null : extraction::ok;
    },
    [&]<class T>(T const& values)
      requires concepts::one_of<T, arrow::Int64Array, arrow::UInt64Array,
                                arrow::DurationArray>
    {
      for (auto i = begin; i < begin + length; ++i) {
        if (values.IsNull(i)) {
          return extraction::has_null;
        }
      }
      if (length == 0) {
        return extraction::ok;
      }
      auto const origin = static_cast<__int128>(values.Value(begin));
      for (auto i = begin; i < begin + length; ++i) {
        auto const offset = static_cast<__int128>(values.Value(i)) - origin;
        out.push_back(static_cast<double>(offset));
      }
      return extraction::ok;
    },
    [&](arrow::DoubleArray const& values) {
      for (auto i = begin; i < begin + length; ++i) {
        if (values.IsNull(i)) {
          return extraction::has_null;
        }
        auto const value = values.Value(i);
        if (not std::isfinite(value)) {
          return extraction::non_finite;
        }
        out.push_back(value);
      }
      return extraction::ok;
    },
    [&](auto const&) {
      return extraction::wrong_type;
    });
}

class autocorrelation_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "autocorrelation";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto max_lag = std::optional<located<int64_t>>{};
    TRY(argument_parser2::function(name())
          .positional("xs", expr, "list")
          .named("max_lag", max_lag, "int")
          .parse(inv, ctx));
    if (max_lag and max_lag->inner < 1) {
      diagnostic::error("`max_lag` must be at least 1")
        .primary(*max_lag)
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make([expr = std::move(expr), max_lag](evaluator eval,
                                                                session ctx) {
      return map_series(eval(expr), [&](series arg) -> series {
        auto const result_type = type{list_type{double_type{}}};
        if (is<null_type>(arg.type)) {
          return series::null(result_type, arg.length());
        }
        auto const list = arg.as<list_type>();
        if (not list) {
          diagnostic::warning("expected `list`, but got `{}`", arg.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(result_type, arg.length());
        }
        auto builder = series_builder{result_type};
        auto xs = std::vector<double>{};
        auto warn_null = false;
        auto warn_non_finite = false;
        auto warn_type = false;
        auto warn_degenerate = false;
        for (auto row = int64_t{0}; row < list->length(); ++row) {
          if (list->array->IsNull(row)) {
            builder.null();
            continue;
          }
          switch (extract_doubles(*list->array, row, xs)) {
            case extraction::has_null:
              warn_null = true;
              builder.null();
              continue;
            case extraction::non_finite:
              warn_non_finite = true;
              builder.null();
              continue;
            case extraction::wrong_type:
              warn_type = true;
              builder.null();
              continue;
            case extraction::ok:
              break;
          }
          if (xs.empty()) {
            builder.list();
            continue;
          }
          auto const n = static_cast<int64_t>(xs.size());
          auto const lag = max_lag ? max_lag->inner : n / 2;
          auto const acf = detail::autocorrelation(xs, lag);
          if (not acf) {
            warn_degenerate = true;
            builder.null();
            continue;
          }
          auto list_builder = builder.list();
          for (auto const r : *acf) {
            list_builder.data(r);
          }
        }
        if (warn_null) {
          diagnostic::warning("list contains null values")
            .note("autocorrelation requires a gap-free series")
            .primary(expr)
            .emit(ctx);
        }
        if (warn_non_finite) {
          diagnostic::warning("list contains non-finite values")
            .note("autocorrelation requires finite samples")
            .primary(expr)
            .emit(ctx);
        }
        if (warn_type) {
          diagnostic::warning(
            "expected list of `int`, `uint`, `double`, or `duration`")
            .primary(expr)
            .emit(ctx);
        }
        if (warn_degenerate) {
          diagnostic::warning("autocorrelation is undefined for constant or "
                              "single-element lists")
            .primary(expr)
            .emit(ctx);
        }
        return builder.finish_assert_one_array();
      });
    });
  }
};

class periodogram_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "periodogram";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("xs", expr, "list")
          .parse(inv, ctx));
    return function_use::make([expr
                               = std::move(expr)](evaluator eval, session ctx) {
      return map_series(eval(expr), [&](series arg) -> series {
        auto const result_type = type{list_type{record_type{
          {"period", double_type{}},
          {"power", double_type{}},
        }}};
        if (is<null_type>(arg.type)) {
          return series::null(result_type, arg.length());
        }
        auto const list = arg.as<list_type>();
        if (not list) {
          diagnostic::warning("expected `list`, but got `{}`", arg.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(result_type, arg.length());
        }
        auto builder = series_builder{result_type};
        auto xs = std::vector<double>{};
        auto warn_null = false;
        auto warn_non_finite = false;
        auto warn_type = false;
        for (auto row = int64_t{0}; row < list->length(); ++row) {
          if (list->array->IsNull(row)) {
            builder.null();
            continue;
          }
          switch (extract_doubles(*list->array, row, xs)) {
            case extraction::has_null:
              warn_null = true;
              builder.null();
              continue;
            case extraction::non_finite:
              warn_non_finite = true;
              builder.null();
              continue;
            case extraction::wrong_type:
              warn_type = true;
              builder.null();
              continue;
            case extraction::ok:
              break;
          }
          auto const result = detail::periodogram(xs);
          if (not result) {
            warn_non_finite = true;
            builder.null();
            continue;
          }
          auto list_builder = builder.list();
          for (size_t k = 1; k <= result->power.size(); ++k) {
            auto record_builder = list_builder.record();
            record_builder.field("period").data(
              static_cast<double>(result->fft_size) / static_cast<double>(k));
            record_builder.field("power").data(result->power[k - 1]);
          }
        }
        if (warn_null) {
          diagnostic::warning("list contains null values")
            .note("periodogram requires a gap-free series")
            .primary(expr)
            .emit(ctx);
        }
        if (warn_non_finite) {
          diagnostic::warning("list contains non-finite values")
            .note("periodogram requires finite samples and powers")
            .primary(expr)
            .emit(ctx);
        }
        if (warn_type) {
          diagnostic::warning(
            "expected list of `int`, `uint`, `double`, or `duration`")
            .primary(expr)
            .emit(ctx);
        }
        return builder.finish_assert_one_array();
      });
    });
  }
};

class dominant_period_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "dominant_period";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto resolution = located<duration>{};
    TRY(argument_parser2::function(name())
          .positional("times", expr, "list")
          .named("resolution", resolution, "duration")
          .parse(inv, ctx));
    if (resolution.inner <= duration::zero()) {
      diagnostic::error("`resolution` must be positive")
        .primary(resolution)
        .emit(ctx);
      return failure::promise();
    }
    return function_use::make([expr = std::move(expr),
                               resolution](evaluator eval, session ctx) {
      return map_series(eval(expr), [&](series arg) -> series {
        auto const result_type = type{record_type{
          {"period", duration_type{}},
          {"strength", double_type{}},
        }};
        if (is<null_type>(arg.type)) {
          return series::null(result_type, arg.length());
        }
        auto const list = arg.as<list_type>();
        if (not list) {
          diagnostic::warning("expected `list`, but got `{}`", arg.type.kind())
            .primary(expr)
            .emit(ctx);
          return series::null(result_type, arg.length());
        }
        auto builder = series_builder{result_type};
        auto const no_peak = [&] {
          auto record_builder = builder.record();
          record_builder.field("period").null();
          record_builder.field("strength").data(0.0);
        };
        auto warn_null = false;
        auto warn_type = false;
        auto warn_bins = false;
        auto warn_period = false;
        auto times = std::vector<int64_t>{};
        for (auto row = int64_t{0}; row < list->length(); ++row) {
          if (list->array->IsNull(row)) {
            builder.null();
            continue;
          }
          auto const begin = list->array->value_offset(row);
          auto const length = list->array->value_length(row);
          times.clear();
          times.reserve(length);
          auto const result = match(
            *list->array->values(),
            [&](arrow::NullArray const&) {
              return length > 0 ? extraction::has_null : extraction::ok;
            },
            [&](arrow::TimestampArray const& values) {
              for (auto i = begin; i < begin + length; ++i) {
                if (values.IsNull(i)) {
                  return extraction::has_null;
                }
                times.push_back(values.Value(i));
              }
              return extraction::ok;
            },
            [&](auto const&) {
              return length == 0 ? extraction::ok : extraction::wrong_type;
            });
          switch (result) {
            case extraction::has_null:
              warn_null = true;
              builder.null();
              continue;
            case extraction::wrong_type:
              warn_type = true;
              builder.null();
              continue;
            case extraction::non_finite:
              TENZIR_UNREACHABLE();
            case extraction::ok:
              break;
          }
          if (times.size() < 2) {
            no_peak();
            continue;
          }
          auto const [min_it, max_it]
            = std::minmax_element(times.begin(), times.end());
          auto const resolution_ns = resolution.inner.count();
          auto const span
            = static_cast<__int128>(*max_it) - static_cast<__int128>(*min_it);
          auto const num_bins_wide = span / resolution_ns + 1;
          if (num_bins_wide > max_bins) {
            warn_bins = true;
            builder.null();
            continue;
          }
          auto const num_bins = static_cast<size_t>(num_bins_wide);
          if (num_bins < 4) {
            no_peak();
            continue;
          }
          auto counts = std::vector<double>(num_bins, 0.0);
          for (auto const t : times) {
            auto const offset
              = static_cast<__int128>(t) - static_cast<__int128>(*min_it);
            auto const bin = static_cast<size_t>(offset / resolution_ns);
            counts[bin] += 1.0;
          }
          auto const peak = detail::dominant_lag(counts);
          if (not peak) {
            no_peak();
            continue;
          }
          auto const period_ns
            = static_cast<__int128>(peak->first) * resolution_ns;
          if (period_ns > std::numeric_limits<int64_t>::max()) {
            warn_period = true;
            builder.null();
            continue;
          }
          auto record_builder = builder.record();
          record_builder.field("period").data(
            duration{static_cast<int64_t>(period_ns)});
          record_builder.field("strength")
            .data(std::clamp(peak->second, 0.0, 1.0));
        }
        if (warn_null) {
          diagnostic::warning("list contains null values")
            .note("dominant_period requires complete timestamp samples")
            .primary(expr)
            .emit(ctx);
        }
        if (warn_type) {
          diagnostic::warning("expected list of `time`").primary(expr).emit(ctx);
        }
        if (warn_bins) {
          diagnostic::warning("`resolution` is too fine for the time span")
            .note("the span must cover at most {} bins", max_bins)
            .primary(expr)
            .emit(ctx);
        }
        if (warn_period) {
          diagnostic::warning("the detected period is too large")
            .note("the period must fit into a 64-bit nanosecond duration")
            .primary(expr)
            .emit(ctx);
        }
        return builder.finish_assert_one_array();
      });
    });
  }
};

} // namespace

} // namespace tenzir::plugins::periodicity

TENZIR_REGISTER_PLUGIN(tenzir::plugins::periodicity::autocorrelation_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::periodicity::periodogram_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::periodicity::dominant_period_plugin)
