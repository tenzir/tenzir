//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/checked_math.hpp>
#include <tenzir/concepts.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/view3.hpp>

#include <cmath>
#include <limits>

namespace tenzir::plugins::exponential {

namespace {

template <class Function>
auto apply_unary(series input, ast::expression const& expr,
                 std::string_view name, session ctx, Function function)
  -> series {
  auto builder = arrow::DoubleBuilder{arrow_memory_pool()};
  check(builder.Reserve(input.length()));
  match(
    *input.array,
    [&](arrow::NullArray const& array) {
      check(builder.AppendNulls(array.length()));
    },
    [&]<
      concepts::one_of<arrow::Int64Array, arrow::UInt64Array, arrow::DoubleArray>
        Array>(Array const& array) {
      for (auto value : values3(array)) {
        if (not value) {
          check(builder.AppendNull());
          continue;
        }
        check(builder.Append(function(static_cast<double>(*value))));
      }
    },
    [&](auto const&) {
      diagnostic::warning("`{}` expected `number`, but got `{}`", name,
                          input.type.kind())
        .primary(expr)
        .emit(ctx);
      check(builder.AppendNulls(input.length()));
    });
  return {double_type{}, finish(builder)};
}

auto checked_pow(int64_t base, int64_t exponent) -> Option<int64_t> {
  if (exponent < 0) {
    return None{};
  }
  auto result = int64_t{1};
  auto remaining = static_cast<uint64_t>(exponent);
  while (remaining > 0) {
    if ((remaining & 1) != 0) {
      auto product = checked_mul(result, base);
      if (not product) {
        return None{};
      }
      result = *product;
    }
    remaining >>= 1;
    if (remaining == 0) {
      break;
    }
    auto square = checked_mul(base, base);
    if (not square) {
      return None{};
    }
    base = *square;
  }
  return result;
}

auto apply_integer_pow(series base, series exponent,
                       ast::expression const& exponent_expr, session ctx)
  -> series {
  TENZIR_ASSERT(is<int64_type>(base.type));
  TENZIR_ASSERT(is<int64_type>(exponent.type));
  TENZIR_ASSERT(base.length() == exponent.length());
  auto const& base_array = as<arrow::Int64Array>(*base.array);
  auto const& exponent_array = as<arrow::Int64Array>(*exponent.array);
  auto builder = arrow::Int64Builder{arrow_memory_pool()};
  check(builder.Reserve(base.length()));
  auto has_negative_exponent = false;
  auto has_overflow = false;
  for (auto row = int64_t{0}; row < base.length(); ++row) {
    if (base_array.IsNull(row) or exponent_array.IsNull(row)) {
      check(builder.AppendNull());
      continue;
    }
    auto const power = exponent_array.Value(row);
    auto result = checked_pow(base_array.Value(row), power);
    if (not result) {
      check(builder.AppendNull());
      has_negative_exponent |= power < 0;
      has_overflow |= power >= 0;
      continue;
    }
    check(builder.Append(*result));
  }
  if (has_negative_exponent) {
    diagnostic::warning("negative exponent in integer `pow`")
      .primary(exponent_expr)
      .emit(ctx);
  }
  if (has_overflow) {
    diagnostic::warning("integer overflow in `pow`")
      .primary(exponent_expr)
      .emit(ctx);
  }
  return {int64_type{}, finish(builder)};
}

template <class Function>
auto apply_binary(series left, series right, ast::expression const& left_expr,
                  ast::expression const& right_expr, std::string_view name,
                  session ctx, Function function) -> series {
  TENZIR_ASSERT(left.length() == right.length());
  auto const is_numeric = [](type const& input) {
    return is<int64_type>(input) or is<uint64_type>(input)
           or is<double_type>(input);
  };
  if (is<null_type>(left.type) or is<null_type>(right.type)) {
    return series::null(double_type{}, left.length());
  }
  if (not is_numeric(left.type)) {
    diagnostic::warning("`{}` expected `number`, but got `{}`", name,
                        left.type.kind())
      .primary(left_expr)
      .emit(ctx);
    return series::null(double_type{}, left.length());
  }
  if (not is_numeric(right.type)) {
    diagnostic::warning("`{}` expected `number`, but got `{}`", name,
                        right.type.kind())
      .primary(right_expr)
      .emit(ctx);
    return series::null(double_type{}, left.length());
  }
  auto builder = arrow::DoubleBuilder{arrow_memory_pool()};
  check(builder.Reserve(left.length()));
  match(
    *left.array,
    [&]<
      concepts::one_of<arrow::Int64Array, arrow::UInt64Array, arrow::DoubleArray>
        Left>(Left const& left_array) {
      match(
        *right.array,
        [&]<concepts::one_of<arrow::Int64Array, arrow::UInt64Array,
                             arrow::DoubleArray>
              Right>(Right const& right_array) {
          for (auto row = int64_t{0}; row < left.length(); ++row) {
            if (left_array.IsNull(row) or right_array.IsNull(row)) {
              check(builder.AppendNull());
              continue;
            }
            check(builder.Append(
              function(static_cast<double>(left_array.Value(row)),
                       static_cast<double>(right_array.Value(row)))));
          }
        },
        [](auto const&) {
          TENZIR_UNREACHABLE();
        });
    },
    [](auto const&) {
      TENZIR_UNREACHABLE();
    });
  return {double_type{}, finish(builder)};
}

template <class T>
concept NovaNumber = concepts::one_of<T, nova::Int, nova::UInt, nova::Float>;

template <class T>
concept NovaNumberOrNull = NovaNumber<T> or std::same_as<T, nova::Null>;

auto log_with_base(double value, double base) -> double {
  if (not std::isfinite(base) or base <= 0.0 or base == 1.0) {
    return std::numeric_limits<double>::quiet_NaN();
  }
  if (base == 2.0) {
    return std::log2(value);
  }
  if (base == 10.0) {
    return std::log10(value);
  }
  return std::log(value) / std::log(base);
}

/// Applies `function` to a single numeric argument, producing a `float`.
template <class Function>
auto apply_nova_unary(nova::EvalFrame frame, std::string_view name,
                      nova::ValueArgument const& x, location call,
                      Function function) -> nova::Array<nova::Data> {
  return nova::apply_kernel<1>(
    frame, name, {x}, call,
    // Constrained to exact types, since `bool` converts implicitly.
    [&]<NovaNumberOrNull T>(diagnostic_handler&, T v) -> Option<nova::Float> {
      if constexpr (std::same_as<T, nova::Null>) {
        return None{};
      } else {
        return function(static_cast<double>(v));
      }
    });
}

/// Applies `function` to two numeric arguments, producing a `float`.
template <class Function>
auto apply_nova_binary(nova::EvalFrame frame, std::string_view name,
                       nova::ValueArgument const& x,
                       nova::ValueArgument const& y, location call,
                       Function function) -> nova::Array<nova::Data> {
  return nova::apply_kernel<2>(
    frame, name, {x, y}, call,
    [&]<NovaNumberOrNull L, NovaNumberOrNull R>(diagnostic_handler&, L l,
                                                R r) -> Option<nova::Float> {
      if constexpr (std::same_as<L, nova::Null>
                    or std::same_as<R, nova::Null>) {
        return None{};
      } else {
        return function(static_cast<double>(l), static_cast<double>(r));
      }
    });
}

struct ExpArgs {
  nova::ValueArgument x;
  location call;
};

struct ExpFunction {
  auto eval(ExpArgs const& args, nova::EvalFrame frame) const
    -> nova::Array<nova::Data> {
    return apply_nova_unary(frame, "exp", args.x, args.call, [](double value) {
      return std::exp(value);
    });
  }
};

struct LogArgs {
  nova::ValueArgument x;
  Option<nova::ValueArgument> base;
  location call;
};

struct LogFunction {
  auto eval(LogArgs const& args, nova::EvalFrame frame) const
    -> nova::Array<nova::Data> {
    if (not args.base) {
      return apply_nova_unary(frame, "log", args.x, args.call,
                              [](double value) {
                                return std::log(value);
                              });
    }
    return apply_nova_binary(frame, "log", args.x, *args.base, args.call,
                             log_with_base);
  }
};

struct PowArgs {
  nova::ValueArgument base;
  nova::ValueArgument exponent;
  location call;
};

struct PowFunction {
  auto eval(PowArgs const& args, nova::EvalFrame frame) const
    -> nova::Array<nova::Data> {
    auto warn_negative = nova::WarnOnce{};
    auto warn_overflow = nova::WarnOnce{};
    return nova::apply_kernel<2>(
      frame, "pow", {args.base, args.exponent}, args.call,
      detail::overload{
        [&]<class L, class R>(diagnostic_handler& dh, L base,
                              R exponent) -> Option<nova::Int>
          requires(std::same_as<L, nova::Int> and std::same_as<R, nova::Int>)
                  {
                    auto result = checked_pow(base, exponent);
                    if (not result) {
                      if (exponent < 0) {
                        warn_negative(dh, diagnostic::warning(
                                            "negative exponent in integer "
                                            "`pow`")
                                            .primary(args.exponent.source));
                      } else {
                        warn_overflow(
                          dh, diagnostic::warning("integer overflow in `pow`")
                                .primary(args.exponent.source));
                      }
                    }
                    return result;
                  },
                  []<NovaNumberOrNull L, NovaNumberOrNull R>(
                    diagnostic_handler&, L base,
                    R exponent) -> Option<nova::Float>
                    requires(not(std::same_as<L, nova::Int>
                                 and std::same_as<R, nova::Int>))
        {
          if constexpr (std::same_as<L, nova::Null>
                        or std::same_as<R, nova::Null>) {
            return None{};
          } else {
            return std::pow(static_cast<double>(base),
                            static_cast<double>(exponent));
          }
        },
        });
  }
};

class exp final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<ExpArgs, ExpFunction>{};
    d.positional("x", &ExpArgs::x, "number");
    d.call_location(&ExpArgs::call);
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "exp";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto x = ast::expression{};
    TRY(argument_parser2::function("exp")
          .positional("x", x, "number")
          .parse(inv, ctx));
    return function_use::make(
      [x = std::move(x)](evaluator eval, session ctx) -> multi_series {
        return map_series(eval(x), [&](series input) {
          return apply_unary(std::move(input), x, "exp", ctx, [](double value) {
            return std::exp(value);
          });
        });
      });
  }
};

class log final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<LogArgs, LogFunction>{};
    d.positional("x", &LogArgs::x, "number");
    d.optional_positional("base", &LogArgs::base, "number");
    d.call_location(&LogArgs::call);
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "log";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto x = ast::expression{};
    auto base = Option<ast::expression>{};
    TRY(argument_parser2::function("log")
          .positional("x", x, "number")
          .positional("base", base, "number")
          .parse(inv, ctx));
    return function_use::make([x = std::move(x), base = std::move(base)](
                                evaluator eval, session ctx) -> multi_series {
      if (not base) {
        return map_series(eval(x), [&](series input) {
          return apply_unary(std::move(input), x, "log", ctx, [](double value) {
            return std::log(value);
          });
        });
      }
      return map_series(
        eval(x), eval(*base), [&](series input, series base_value) {
          return apply_binary(std::move(input), std::move(base_value), x, *base,
                              "log", ctx, log_with_base);
        });
    });
  }
};

class pow final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<PowArgs, PowFunction>{};
    d.positional("base", &PowArgs::base, "number");
    d.positional("exponent", &PowArgs::exponent, "number");
    d.call_location(&PowArgs::call);
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "pow";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto base = ast::expression{};
    auto exponent = ast::expression{};
    TRY(argument_parser2::function("pow")
          .positional("base", base, "number")
          .positional("exponent", exponent, "number")
          .parse(inv, ctx));
    return function_use::make([base = std::move(base), exponent
                                                       = std::move(exponent)](
                                evaluator eval, session ctx) -> multi_series {
      return map_series(
        eval(base), eval(exponent), [&](series base_value, series power) {
          if (is<int64_type>(base_value.type) and is<int64_type>(power.type)) {
            return apply_integer_pow(std::move(base_value), std::move(power),
                                     exponent, ctx);
          }
          return apply_binary(std::move(base_value), std::move(power), base,
                              exponent, "pow", ctx,
                              [](double base, double exponent) {
                                return std::pow(base, exponent);
                              });
        });
    });
  }
};

} // namespace

} // namespace tenzir::plugins::exponential

TENZIR_REGISTER_PLUGIN(tenzir::plugins::exponential::exp)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::exponential::log)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::exponential::pow)
