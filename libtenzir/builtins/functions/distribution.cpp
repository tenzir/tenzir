//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/detail/distribution.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>

namespace tenzir::plugins::distribution {

namespace {

enum class extraction { ok, has_null, non_finite, inexact, wrong_type };

constexpr auto max_exact_integer = uint64_t{1} << 53;

auto within_exact_integer_range(data_view3 value) -> bool {
  if (auto const* x = try_as<int64_t>(value)) {
    return *x >= -static_cast<int64_t>(max_exact_integer)
           and *x <= static_cast<int64_t>(max_exact_integer);
  }
  if (auto const* x = try_as<uint64_t>(value)) {
    return *x <= max_exact_integer;
  }
  return true;
}

auto number_to_double(data_view3 value) -> Option<double> {
  if (auto const* x = try_as<double>(value)) {
    return *x;
  }
  if (auto const* x = try_as<int64_t>(value)) {
    return static_cast<double>(*x);
  }
  if (auto const* x = try_as<uint64_t>(value)) {
    return static_cast<double>(*x);
  }
  return None{};
}

auto temporal_to_int64(data_view3 value, bool timestamp) -> Option<int64_t> {
  if (timestamp) {
    if (auto const* x = try_as<time>(value)) {
      return x->time_since_epoch().count();
    }
    return None{};
  }
  if (auto const* x = try_as<duration>(value)) {
    return x->count();
  }
  return None{};
}

auto extract_numbers(view3<list> row, std::vector<double>& out,
                     bool exact_integers) -> extraction {
  out.clear();
  out.reserve(row.size());
  for (auto value : row) {
    if (is<caf::none_t>(value)) {
      return extraction::has_null;
    }
    if (exact_integers and not within_exact_integer_range(value)) {
      return extraction::inexact;
    }
    auto const number = number_to_double(value);
    if (not number) {
      return extraction::wrong_type;
    }
    if (not std::isfinite(*number)) {
      return extraction::non_finite;
    }
    out.push_back(*number);
  }
  return extraction::ok;
}

auto extract_temporal(view3<list> row, bool timestamp,
                      std::vector<int64_t>& out) -> extraction {
  out.clear();
  out.reserve(row.size());
  for (auto value : row) {
    if (is<caf::none_t>(value)) {
      return extraction::has_null;
    }
    auto const temporal = temporal_to_int64(value, timestamp);
    if (not temporal) {
      return extraction::wrong_type;
    }
    out.push_back(*temporal);
  }
  return extraction::ok;
}

auto warn_once(bool& warned, std::string_view message,
               ast::expression const& expr, session ctx) -> void {
  if (warned) {
    return;
  }
  warned = true;
  diagnostic::warning("{}", message).primary(expr).emit(ctx);
}

auto extract_or_warn(view3<list> row, std::vector<double>& out, bool& warned,
                     ast::expression const& expr, session ctx,
                     bool exact_integers = false) -> bool {
  switch (extract_numbers(row, out, exact_integers)) {
    case extraction::ok:
      return true;
    case extraction::has_null:
      warn_once(warned, "distribution samples must not contain nulls", expr,
                ctx);
      return false;
    case extraction::non_finite:
      warn_once(warned, "distribution samples must be finite", expr, ctx);
      return false;
    case extraction::inexact:
      warn_once(warned, "distribution integers must be between -2^53 and 2^53",
                expr, ctx);
      return false;
    case extraction::wrong_type:
      warn_once(warned, "distribution samples must be numbers", expr, ctx);
      return false;
  }
  TENZIR_UNREACHABLE();
}

class jensen_shannon_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "jensen_shannon";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto lhs = ast::expression{};
    auto rhs = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("p", lhs, "list")
          .positional("q", rhs, "list")
          .parse(inv, ctx));
    return function_use::make([lhs = std::move(lhs), rhs = std::move(rhs)](
                                evaluator eval, session ctx) {
      return map_series(
        eval(lhs), eval(rhs), [&](series p, series q) -> series {
          auto builder = double_type::make_arrow_builder(arrow_memory_pool());
          auto const p_lists = p.as<list_type>();
          auto const q_lists = q.as<list_type>();
          if (not p_lists or not q_lists) {
            check(builder->AppendNulls(p.length()));
            return series{double_type{}, finish(*builder)};
          }
          auto p_values = std::vector<double>{};
          auto q_values = std::vector<double>{};
          auto p_rows = values3(*p_lists->array);
          auto q_rows = values3(*q_lists->array);
          auto p_row = p_rows.begin();
          auto q_row = q_rows.begin();
          auto warned = false;
          for (; p_row != p_rows.end(); ++p_row, ++q_row) {
            if (not *p_row or not *q_row) {
              check(builder->AppendNull());
              continue;
            }
            if (not extract_or_warn(**p_row, p_values, warned, lhs, ctx)
                or not extract_or_warn(**q_row, q_values, warned, rhs, ctx)) {
              check(builder->AppendNull());
              continue;
            }
            if (p_values.size() != q_values.size()) {
              warn_once(warned,
                        "Jensen-Shannon weight lists must have equal lengths",
                        lhs, ctx);
              check(builder->AppendNull());
              continue;
            }
            if (std::ranges::any_of(p_values,
                                    [](double x) {
                                      return x < 0.0;
                                    })
                or std::ranges::any_of(q_values, [](double x) {
                     return x < 0.0;
                   })) {
              warn_once(warned, "Jensen-Shannon weights must be non-negative",
                        lhs, ctx);
              check(builder->AppendNull());
              continue;
            }
            auto const has_positive_weight = [](auto const& values) {
              return std::ranges::any_of(values, [](double x) {
                return x > 0.0;
              });
            };
            if (not has_positive_weight(p_values)
                or not has_positive_weight(q_values)) {
              check(builder->AppendNull());
              continue;
            }
            check(builder->Append(detail::jensen_shannon(p_values, q_values)));
          }
          return series{double_type{}, finish(*builder)};
        });
    });
  }
};

class ecdf_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "ecdf";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto samples = ast::expression{};
    auto x = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("samples", samples, "list")
          .positional("x", x, "number|duration|time")
          .parse(inv, ctx));
    return function_use::make([samples = std::move(samples),
                               x = std::move(x)](evaluator eval, session ctx) {
      return map_series(
        eval(samples), eval(x), [&](series input, series points) -> series {
          auto builder = double_type::make_arrow_builder(arrow_memory_pool());
          auto const lists = input.as<list_type>();
          if (not lists) {
            check(builder->AppendNulls(input.length()));
            return series{double_type{}, finish(*builder)};
          }
          auto point_values = values3(*points.array);
          auto rows = values3(*lists->array);
          auto point = point_values.begin();
          auto row = rows.begin();
          auto values = std::vector<double>{};
          auto temporal_values = std::vector<int64_t>{};
          auto const timestamp = is<time_type>(lists->type.value_type());
          auto const temporal
            = timestamp or is<duration_type>(lists->type.value_type());
          auto warned = false;
          for (; row != rows.end(); ++row, ++point) {
            if (not *row or is<caf::none_t>(*point)) {
              check(builder->AppendNull());
              continue;
            }
            if (temporal) {
              auto const query = temporal_to_int64(*point, timestamp);
              if (not query) {
                warn_once(warned,
                          "ECDF query values must match the sample type", x,
                          ctx);
                check(builder->AppendNull());
                continue;
              }
              if (extract_temporal(**row, timestamp, temporal_values)
                  != extraction::ok) {
                warn_once(warned, "distribution samples must not contain nulls",
                          samples, ctx);
                check(builder->AppendNull());
                continue;
              }
              if (temporal_values.empty()) {
                check(builder->AppendNull());
                continue;
              }
              check(builder->Append(detail::ecdf(temporal_values, *query)));
              continue;
            }
            if (not within_exact_integer_range(*point)) {
              warn_once(warned,
                        "ECDF query integers must be between -2^53 and 2^53", x,
                        ctx);
              check(builder->AppendNull());
              continue;
            }
            auto const query = number_to_double(*point);
            if (not query or not std::isfinite(*query)) {
              warn_once(warned, "ECDF query values must be finite numbers", x,
                        ctx);
              check(builder->AppendNull());
              continue;
            }
            if (not extract_or_warn(**row, values, warned, samples, ctx,
                                    true)) {
              check(builder->AppendNull());
              continue;
            }
            if (values.empty()) {
              check(builder->AppendNull());
              continue;
            }
            check(builder->Append(detail::ecdf(values, *query)));
          }
          return series{double_type{}, finish(*builder)};
        });
    });
  }
};

enum class distance_kind { kolmogorov_smirnov, wasserstein };

template <distance_kind Kind>
class distance_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    if constexpr (Kind == distance_kind::kolmogorov_smirnov) {
      return "kolmogorov_smirnov";
    }
    return "wasserstein";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto lhs = ast::expression{};
    auto rhs = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", lhs, "list")
          .positional("y", rhs, "list")
          .parse(inv, ctx));
    return function_use::make([lhs = std::move(lhs), rhs = std::move(rhs)](
                                evaluator eval, session ctx) {
      return map_series(eval(lhs), eval(rhs), [&](series x, series y) -> series {
        auto const x_lists = x.as<list_type>();
        auto const y_lists = y.as<list_type>();
        if (not x_lists or not y_lists) {
          return series::null(double_type{}, x.length());
        }
        auto const temporal_type = x_lists->type.value_type();
        auto const temporal = temporal_type == y_lists->type.value_type()
                              and (is<duration_type>(temporal_type)
                                   or is<time_type>(temporal_type));
        if (temporal) {
          auto const timestamp = is<time_type>(temporal_type);
          auto run = [&]<class Output>(Output output, auto compute) -> series {
            auto builder = Output::make_arrow_builder(arrow_memory_pool());
            auto x_rows = values3(*x_lists->array);
            auto y_rows = values3(*y_lists->array);
            auto x_row = x_rows.begin();
            auto y_row = y_rows.begin();
            auto x_values = std::vector<int64_t>{};
            auto y_values = std::vector<int64_t>{};
            auto warned = false;
            for (; x_row != x_rows.end(); ++x_row, ++y_row) {
              if (not *x_row or not *y_row) {
                check(builder->AppendNull());
                continue;
              }
              if (extract_temporal(**x_row, timestamp, x_values)
                    != extraction::ok
                  or extract_temporal(**y_row, timestamp, y_values)
                       != extraction::ok) {
                warn_once(warned, "distribution samples must not contain nulls",
                          lhs, ctx);
                check(builder->AppendNull());
                continue;
              }
              if (x_values.empty() or y_values.empty()) {
                check(builder->AppendNull());
                continue;
              }
              std::ranges::sort(x_values);
              std::ranges::sort(y_values);
              auto const value = compute(x_values, y_values);
              if constexpr (std::same_as<Output, duration_type>) {
                if (value >= static_cast<double>(
                      std::numeric_limits<duration::rep>::max())) {
                  warn_once(warned,
                            "Wasserstein distance exceeds duration range", lhs,
                            ctx);
                  check(builder->AppendNull());
                  continue;
                }
                check(builder->Append(static_cast<duration::rep>(value)));
              } else {
                check(builder->Append(value));
              }
            }
            return series{output, finish(*builder)};
          };
          if constexpr (Kind == distance_kind::kolmogorov_smirnov) {
            return run(double_type{}, [](auto const& lhs, auto const& rhs) {
              return detail::kolmogorov_smirnov(lhs, rhs);
            });
          } else {
            return run(duration_type{}, [](auto const& lhs, auto const& rhs) {
              return detail::wasserstein(lhs, rhs);
            });
          }
        }
        auto builder = double_type::make_arrow_builder(arrow_memory_pool());
        auto x_values = std::vector<double>{};
        auto y_values = std::vector<double>{};
        auto x_rows = values3(*x_lists->array);
        auto y_rows = values3(*y_lists->array);
        auto x_row = x_rows.begin();
        auto y_row = y_rows.begin();
        auto warned = false;
        for (; x_row != x_rows.end(); ++x_row, ++y_row) {
          if (not *x_row or not *y_row) {
            check(builder->AppendNull());
            continue;
          }
          if (not extract_or_warn(**x_row, x_values, warned, lhs, ctx, true)
              or not extract_or_warn(**y_row, y_values, warned, rhs, ctx,
                                     true)) {
            check(builder->AppendNull());
            continue;
          }
          if (x_values.empty() or y_values.empty()) {
            check(builder->AppendNull());
            continue;
          }
          std::ranges::sort(x_values);
          std::ranges::sort(y_values);
          if constexpr (Kind == distance_kind::kolmogorov_smirnov) {
            check(
              builder->Append(detail::kolmogorov_smirnov(x_values, y_values)));
          } else {
            check(builder->Append(detail::wasserstein(x_values, y_values)));
          }
        }
        return series{double_type{}, finish(*builder)};
      });
    });
  }
};

using kolmogorov_smirnov_plugin
  = distance_plugin<distance_kind::kolmogorov_smirnov>;
using wasserstein_plugin = distance_plugin<distance_kind::wasserstein>;

} // namespace

} // namespace tenzir::plugins::distribution

TENZIR_REGISTER_PLUGIN(tenzir::plugins::distribution::jensen_shannon_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::distribution::ecdf_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::distribution::kolmogorov_smirnov_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::distribution::wasserstein_plugin)
