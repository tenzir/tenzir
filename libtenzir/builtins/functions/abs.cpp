//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/nova/eval_kernel.hpp>
#include <tenzir/nova/function_plugin.hpp>
#include <tenzir/plugin/register.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/compute/api.h>
#include <arrow/type_fwd.h>

namespace tenzir::plugins::abs {

namespace {

struct AbsArgs {
  nova::ValueArgument x;
  location call;
};

struct AbsFunction {
  auto eval(AbsArgs const& args, nova::EvalFrame frame) const
    -> nova::Array<nova::Data> {
    auto integer_overflow = nova::WarnOnce{};
    auto duration_overflow = nova::WarnOnce{};
    return nova::apply_kernel<1>(
      frame, "abs", {args.x}, args.call,
      detail::overload{
        [](diagnostic_handler&, nova::Null) -> Option<nova::Int> {
          return None{};
        },
        [](diagnostic_handler&, nova::UInt v) -> Option<nova::UInt> {
          return v;
        },
        [](diagnostic_handler&, nova::Float v) -> Option<nova::Float> {
          return std::abs(v);
        },
        [&](diagnostic_handler& dh, nova::Int v) -> Option<nova::Int> {
          if (v == std::numeric_limits<nova::Int>::lowest()) {
            integer_overflow(
              dh,
              diagnostic::warning("integer overflow").primary(args.x.source));
            return None{};
          }
          return std::abs(v);
        },
        [&](diagnostic_handler& dh,
            nova::Duration v) -> Option<nova::Duration> {
          if (v.count() == std::numeric_limits<nova::Duration::rep>::lowest()) {
            duration_overflow(
              dh,
              diagnostic::warning("duration overflow").primary(args.x.source));
            return None{};
          }
          return nova::Duration{std::abs(v.count())};
        }});
  }
};

class abs final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<AbsArgs, AbsFunction>{};
    d.positional("x", &AbsArgs::x, "duration|number");
    d.call_location(&AbsArgs::call);
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "abs";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "duration|number")
          .parse(inv, ctx));
    return function_use::make([expr = std::move(expr)](
                                evaluator eval, session ctx) -> multi_series {
      const auto evaluated = eval(expr);
      return map_series(evaluated, [&](series arg) {
        return match(
          *arg.array,
          [&](const arrow::NullArray&) -> series {
            return arg;
          },
          [&](const arrow::UInt64Array&) -> series {
            return arg;
          },
          [&](const arrow::Int64Array& array) -> series {
            // TODO: Maybe slice the array for positive values
            auto overflow = false;
            auto b = int64_type::make_arrow_builder(arrow_memory_pool());
            for (auto v : values(int64_type{}, array)) {
              if (not v) {
                check(b->AppendNull());
                continue;
              }
              const auto val = v.value();
              if (val == std::numeric_limits<int64_t>::lowest()) {
                check(b->AppendNull());
                overflow = true;
                continue;
              }
              check(b->Append(std::abs(val)));
            }
            if (overflow) {
              diagnostic::warning("integer overflow").primary(expr).emit(ctx);
            }
            return series{int64_type{}, finish(*b)};
          },
          [&](const arrow::DoubleArray& array) -> series {
            // TODO: Maybe slice the array for positive values
            auto b = double_type::make_arrow_builder(arrow_memory_pool());
            for (auto v : values(double_type{}, array)) {
              if (not v) {
                check(b->AppendNull());
                continue;
              }
              check(b->Append(std::abs(v.value())));
            }
            return series{double_type{}, finish(*b)};
          },
          [&](const arrow::DurationArray& array) {
            // TODO: Maybe slice the array for positive values
            auto overflow = false;
            auto b = duration_type::make_arrow_builder(arrow_memory_pool());
            for (auto v : values(duration_type{}, array)) {
              if (not v) {
                check(b->AppendNull());
                continue;
              }
              const auto val = v->count();
              if (val == std::numeric_limits<duration::rep>::lowest()) {
                check(b->AppendNull());
                overflow = true;
                continue;
              }
              check(b->Append(std::abs(val)));
            }
            if (overflow) {
              diagnostic::warning("duration overflow").primary(expr).emit(ctx);
            }
            return series{duration_type{}, finish(*b)};
          },
          [&](const auto&) {
            diagnostic::warning("expected `duration|number`, but got `{}`",
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(null_type{}, arg.length());
          });
      });
    });
  }
};

} // namespace

} // namespace tenzir::plugins::abs

TENZIR_REGISTER_PLUGIN(tenzir::plugins::abs::abs)
