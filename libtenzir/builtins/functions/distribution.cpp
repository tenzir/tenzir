//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/detail/distribution.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/nova/list_util.hpp>
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

struct DistributionArgs {
  nova::ValueArgument lhs;
  nova::ValueArgument rhs;
};

auto extract_numbers(nova::RowView<nova::List> row, std::vector<double>& out,
                     bool exact_integers) -> extraction {
  using namespace nova;
  out.clear();
  out.reserve(row.length());
  for (auto value : row) {
    auto result = extraction::wrong_type;
    match(
      value,
      [&](RowView<Null>) {
        result = extraction::has_null;
      },
      [&](RowView<Int> x) {
        if (exact_integers
            and (*x < -static_cast<int64_t>(max_exact_integer)
                 or *x > static_cast<int64_t>(max_exact_integer))) {
          result = extraction::inexact;
        } else {
          out.push_back(static_cast<double>(*x));
          result = extraction::ok;
        }
      },
      [&](RowView<UInt> x) {
        if (exact_integers and *x > max_exact_integer) {
          result = extraction::inexact;
        } else {
          out.push_back(static_cast<double>(*x));
          result = extraction::ok;
        }
      },
      [&](RowView<Float> x) {
        if (std::isfinite(*x)) {
          out.push_back(*x);
          result = extraction::ok;
        } else {
          result = extraction::non_finite;
        }
      },
      [&](auto const&) {});
    if (result != extraction::ok) {
      return result;
    }
  }
  return extraction::ok;
}

auto extract_temporal(nova::RowView<nova::List> row, bool timestamp,
                      std::vector<int64_t>& out) -> extraction {
  using namespace nova;
  out.clear();
  out.reserve(row.length());
  for (auto value : row) {
    auto result = extraction::wrong_type;
    match(
      value,
      [&](RowView<Null>) {
        result = extraction::has_null;
      },
      [&](RowView<Time> x) {
        if (timestamp) {
          out.push_back((*x).time_since_epoch().count());
          result = extraction::ok;
        }
      },
      [&](RowView<Duration> x) {
        if (not timestamp) {
          out.push_back((*x).count());
          result = extraction::ok;
        }
      },
      [&](auto const&) {});
    if (result != extraction::ok) {
      return result;
    }
  }
  return extraction::ok;
}

auto finish_nova(nova::ArrayBuilder<nova::Data>& builder,
                 nova::EvalFrame const& frame,
                 nova::storage::BitMap const& rows) -> nova::Array<nova::Data> {
  builder.skip_n(frame.length() - builder.length());
  return builder.finish().null_where(frame.mask().and_not(rows));
}

enum class distance_kind { kolmogorov_smirnov, wasserstein };

class JensenShannonFunction final {
public:
  static auto eval(DistributionArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto lhs = nova::resolve_list(args.lhs, frame);
    auto rhs = nova::resolve_list(args.rhs, frame);
    if (not lhs or not rhs) {
      return frame.null();
    }
    auto rows = lhs->present & rhs->present;
    auto builder = nova::ArrayBuilder<nova::Data>{};
    auto warned = nova::WarnOnce{};
    auto p = std::vector<double>{};
    auto q = std::vector<double>{};
    for (auto row : nova::storage::true_bits(rows)) {
      builder.skip_n(row - builder.length());
      auto const fail = [&](std::string_view message, location source) {
        warned(frame, diagnostic::warning("{}", message).primary(source));
        builder.null();
      };
      auto const p_status = extract_numbers(lhs->data.get(row), p, false);
      auto const q_status = extract_numbers(rhs->data.get(row), q, false);
      if (p_status != extraction::ok or q_status != extraction::ok) {
        auto const status = p_status != extraction::ok ? p_status : q_status;
        auto const message = [&] {
          switch (status) {
            case extraction::has_null:
              return "distribution samples must not contain nulls";
            case extraction::non_finite:
              return "distribution samples must be finite";
            case extraction::inexact:
              return "distribution integers must be between -2^53 and 2^53";
            case extraction::wrong_type:
              return "distribution samples must be numbers";
            case extraction::ok:
              TENZIR_UNREACHABLE();
          }
          TENZIR_UNREACHABLE();
        }();
        fail(message,
             p_status != extraction::ok ? args.lhs.source : args.rhs.source);
      } else if (p.size() != q.size()) {
        fail("Jensen-Shannon weight lists must have equal lengths",
             args.lhs.source);
      } else if (std::ranges::any_of(p,
                                     [](auto x) {
                                       return x < 0;
                                     })
                 or std::ranges::any_of(q, [](auto x) {
                      return x < 0;
                    })) {
        fail("Jensen-Shannon weights must be non-negative", args.lhs.source);
      } else if (not std::ranges::any_of(p,
                                         [](auto x) {
                                           return x > 0;
                                         })
                 or not std::ranges::any_of(q, [](auto x) {
                      return x > 0;
                    })) {
        builder.null();
      } else {
        nova::append_data(builder, nova::Data{detail::jensen_shannon(p, q)});
      }
    }
    return finish_nova(builder, frame, rows);
  }
};

class EcdfFunction final {
public:
  static auto eval(DistributionArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto samples = nova::resolve_list(args.lhs, frame);
    if (not samples) {
      return frame.null();
    }
    auto builder = nova::ArrayBuilder<nova::Data>{};
    auto warned = nova::WarnOnce{};
    auto numbers = std::vector<double>{};
    auto temporal = std::vector<int64_t>{};
    for (auto row : nova::storage::true_bits(samples->present)) {
      builder.skip_n(row - builder.length());
      auto const list = samples->data.get(row);
      if (list.length() == 0) {
        builder.null();
        continue;
      }
      auto const point = args.rhs.data.get(row);
      if (is<nova::RowView<nova::Null>>(point)) {
        builder.null();
        continue;
      }
      auto const temporal_kind = [&] {
        auto result = Option<bool>{};
        for (auto value : list) {
          match(
            value,
            [&](nova::RowView<nova::Time>) {
              result = true;
            },
            [&](nova::RowView<nova::Duration>) {
              result = false;
            },
            [&](auto const&) {});
          if (result) {
            break;
          }
        }
        return result;
      }();
      if (temporal_kind) {
        auto query = Option<int64_t>{};
        match(
          point,
          [&](nova::RowView<nova::Time> x) {
            if (*temporal_kind) {
              query = (*x).time_since_epoch().count();
            }
          },
          [&](nova::RowView<nova::Duration> x) {
            if (not *temporal_kind) {
              query = (*x).count();
            }
          },
          [&](auto const&) {});
        if (not query) {
          warned(frame, diagnostic::warning("ECDF query values must match the "
                                            "sample type")
                          .primary(args.rhs.source));
          builder.null();
        } else if (auto const status
                   = extract_temporal(list, *temporal_kind, temporal);
                   status != extraction::ok) {
          auto const message = [&] {
            switch (status) {
              case extraction::has_null:
                return "distribution samples must not contain nulls";
              case extraction::wrong_type:
                return "ECDF samples must have matching temporal types";
              case extraction::ok:
              case extraction::non_finite:
              case extraction::inexact:
                TENZIR_UNREACHABLE();
            }
            TENZIR_UNREACHABLE();
          }();
          warned(frame,
                 diagnostic::warning("{}", message).primary(args.lhs.source));
          builder.null();
        } else if (temporal.empty()) {
          builder.null();
        } else {
          nova::append_data(builder,
                            nova::Data{detail::ecdf(temporal, *query)});
        }
        continue;
      }
      auto query = Option<double>{};
      auto valid = true;
      match(
        point,
        [&](nova::RowView<nova::Int> x) {
          valid = *x >= -static_cast<int64_t>(max_exact_integer)
                  and *x <= static_cast<int64_t>(max_exact_integer);
          query = static_cast<double>(*x);
        },
        [&](nova::RowView<nova::UInt> x) {
          valid = *x <= max_exact_integer;
          query = static_cast<double>(*x);
        },
        [&](nova::RowView<nova::Float> x) {
          query = *x;
        },
        [&](auto const&) {});
      if (not valid) {
        warned(frame, diagnostic::warning("ECDF query integers must be between "
                                          "-2^53 and 2^53")
                        .primary(args.rhs.source));
        builder.null();
      } else if (not query or not std::isfinite(*query)) {
        warned(frame,
               diagnostic::warning("ECDF query values must be finite numbers")
                 .primary(args.rhs.source));
        builder.null();
      } else if (auto const status = extract_numbers(list, numbers, true);
                 status != extraction::ok) {
        auto const message = [&] {
          switch (status) {
            case extraction::has_null:
              return "distribution samples must not contain nulls";
            case extraction::non_finite:
              return "distribution samples must be finite";
            case extraction::inexact:
              return "distribution integers must be between -2^53 and 2^53";
            case extraction::wrong_type:
              return "distribution samples must be numbers";
            case extraction::ok:
              TENZIR_UNREACHABLE();
          }
          TENZIR_UNREACHABLE();
        }();
        warned(frame,
               diagnostic::warning("{}", message).primary(args.lhs.source));
        builder.null();
      } else if (numbers.empty()) {
        builder.null();
      } else {
        nova::append_data(builder, nova::Data{detail::ecdf(numbers, *query)});
      }
    }
    return finish_nova(builder, frame, samples->present);
  }
};

template <distance_kind Kind>
class DistanceFunction final {
public:
  static auto eval(DistributionArgs const& args, nova::EvalFrame frame)
    -> nova::Array<nova::Data> {
    auto lhs = nova::resolve_list(args.lhs, frame);
    auto rhs = nova::resolve_list(args.rhs, frame);
    if (not lhs or not rhs) {
      return frame.null();
    }
    auto rows = lhs->present & rhs->present;
    auto builder = nova::ArrayBuilder<nova::Data>{};
    auto warned = nova::WarnOnce{};
    auto x = std::vector<double>{};
    auto y = std::vector<double>{};
    auto temporal_x = std::vector<int64_t>{};
    auto temporal_y = std::vector<int64_t>{};
    for (auto row : nova::storage::true_bits(rows)) {
      builder.skip_n(row - builder.length());
      auto const xs = lhs->data.get(row);
      auto const ys = rhs->data.get(row);
      auto const duration_x = extract_temporal(xs, false, temporal_x);
      auto const duration_y = extract_temporal(ys, false, temporal_y);
      if (duration_x == extraction::has_null
          or duration_y == extraction::has_null) {
        warned(frame,
               diagnostic::warning("distribution samples must not contain "
                                   "nulls")
                 .primary(duration_x == extraction::has_null
                            ? args.lhs.source
                            : args.rhs.source));
        builder.null();
        continue;
      }
      auto temporal
        = duration_x == extraction::ok and duration_y == extraction::ok;
      if (not temporal) {
        auto const time_x = extract_temporal(xs, true, temporal_x);
        auto const time_y = extract_temporal(ys, true, temporal_y);
        if (time_x == extraction::has_null or time_y == extraction::has_null) {
          warned(frame,
                 diagnostic::warning("distribution samples must not "
                                     "contain nulls")
                   .primary(time_x == extraction::has_null ? args.lhs.source
                                                           : args.rhs.source));
          builder.null();
          continue;
        }
        temporal = time_x == extraction::ok and time_y == extraction::ok;
      }
      if (temporal) {
        if (temporal_x.empty() or temporal_y.empty()) {
          builder.null();
          continue;
        }
        std::ranges::sort(temporal_x);
        std::ranges::sort(temporal_y);
        if constexpr (Kind == distance_kind::kolmogorov_smirnov) {
          nova::append_data(builder, nova::Data{detail::kolmogorov_smirnov(
                                       temporal_x, temporal_y)});
        } else {
          auto const result = detail::wasserstein(temporal_x, temporal_y);
          if (result >= static_cast<double>(
                std::numeric_limits<duration::rep>::max())) {
            warned(frame, diagnostic::warning("Wasserstein distance exceeds "
                                              "duration range")
                            .primary(args.lhs.source));
            builder.null();
          } else {
            nova::append_data(builder, nova::Data{duration{
                                         static_cast<duration::rep>(result)}});
          }
        }
        continue;
      }
      auto const x_status = extract_numbers(xs, x, true);
      auto const y_status = extract_numbers(ys, y, true);
      if (x_status != extraction::ok or y_status != extraction::ok) {
        auto const status = x_status != extraction::ok ? x_status : y_status;
        auto const message = [&] {
          switch (status) {
            case extraction::has_null:
              return "distribution samples must not contain nulls";
            case extraction::non_finite:
              return "distribution samples must be finite";
            case extraction::inexact:
              return "distribution integers must be between -2^53 and 2^53";
            case extraction::wrong_type:
              return "distribution samples must be numbers";
            case extraction::ok:
              TENZIR_UNREACHABLE();
          }
          TENZIR_UNREACHABLE();
        }();
        warned(frame, diagnostic::warning("{}", message)
                        .primary(x_status != extraction::ok ? args.lhs.source
                                                            : args.rhs.source));
        builder.null();
        continue;
      }
      if (x.empty() or y.empty()) {
        builder.null();
        continue;
      }
      std::ranges::sort(x);
      std::ranges::sort(y);
      if constexpr (Kind == distance_kind::kolmogorov_smirnov) {
        nova::append_data(builder,
                          nova::Data{detail::kolmogorov_smirnov(x, y)});
      } else {
        nova::append_data(builder, nova::Data{detail::wasserstein(x, y)});
      }
    }
    return finish_nova(builder, frame, rows);
  }
};

class jensen_shannon_plugin final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "jensen_shannon";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<DistributionArgs, JensenShannonFunction>{};
    d.positional("p", &DistributionArgs::lhs, "list");
    d.positional("q", &DistributionArgs::rhs, "list");
    return std::move(d).finish();
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

class ecdf_plugin final : public nova::FunctionPlugin {
public:
  auto name() const -> std::string override {
    return "ecdf";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<DistributionArgs, EcdfFunction>{};
    d.positional("samples", &DistributionArgs::lhs, "list");
    d.positional("x", &DistributionArgs::rhs, "number|duration|time");
    return std::move(d).finish();
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

template <distance_kind Kind>
class distance_plugin final : public nova::FunctionPlugin {
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

  auto describe() const -> nova::FunctionDescription override {
    auto d
      = nova::FunctionDescriber<DistributionArgs, DistanceFunction<Kind>>{};
    d.positional("x", &DistributionArgs::lhs, "list");
    d.positional("y", &DistributionArgs::rhs, "list");
    return std::move(d).finish();
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
