//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/eval_kernel.hpp"
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/tql2/plugin.hpp"

namespace tenzir::plugins::float_ {

namespace {

struct FloatArgs {
  nova::ValueArgument x;
  location call;
};

struct FloatFunction {
  auto eval(FloatArgs const& args, nova::EvalFrame frame) const
    -> nova::Array<nova::Data> {
    auto warn_parse = nova::WarnOnce{};
    return nova::apply_kernel<1>(
      frame, "float", {args.x}, args.call,
      detail::overload{
        [](diagnostic_handler&, nova::Null) -> Option<nova::Float> {
          return None{};
        },
        // Constrained to exact types, since `bool` converts implicitly.
        []<class T>(diagnostic_handler&, T v) -> Option<nova::Float>
          requires concepts::one_of<T, nova::Int, nova::UInt, nova::Float>
        {
          return static_cast<nova::Float>(v);
        },
        [&](diagnostic_handler& dh, std::string_view v) -> Option<nova::Float> {
          constexpr auto p = ignore(*parsers::space) >> parsers::number
                             >> ignore(*parsers::space);
          auto result = double{};
          if (p(v, result)) {
            return result;
          }
          warn_parse(dh, diagnostic::warning("failed to parse string")
                           .primary(args.x.source)
                           .note("tried to convert: {}", v));
          return None{};
        },
        });
  }
};

class float_ final : public nova::FunctionPlugin {
public:
  auto describe() const -> nova::FunctionDescription override {
    auto d = nova::FunctionDescriber<FloatArgs, FloatFunction>{};
    d.positional("x", &FloatArgs::x, "number|string");
    d.call_location(&FloatArgs::call);
    return std::move(d).finish();
  }

  auto name() const -> std::string override {
    return "float";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number|string")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), this](auto eval, session ctx) {
        auto failed = Option<std::string>{};
        auto result = map_series(eval(expr), [&](series value) {
          const auto f = detail::overload{
            [](const arrow::NullArray& arg) {
              auto b = arrow::DoubleBuilder{tenzir::arrow_memory_pool()};
              check(b.AppendNulls(arg.length()));
              return finish(b);
            },
            [](const arrow::DoubleArray& arg) {
              return std::make_shared<arrow::DoubleArray>(arg.data());
            },
            [&]<class T>(const T& arg)
              requires integral_type<type_from_arrow_t<T>>
            {
              auto b = arrow::DoubleBuilder{tenzir::arrow_memory_pool()};
              check(b.Reserve(arg.length()));
              for (auto i = int64_t{0}; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  check(b.AppendNull());
                  continue;
                }
                check(b.Append(static_cast<double>(arg.Value(i))));
              }
              return finish(b);
            },
            [&](const arrow::StringArray& arg) {
              auto b = arrow::DoubleBuilder{tenzir::arrow_memory_pool()};
              check(b.Reserve(value.length()));
              for (auto row = int64_t{0}; row < value.length(); ++row) {
                if (arg.IsNull(row)) {
                  check(b.AppendNull());
                  continue;
                }
                constexpr auto p = ignore(*parsers::space) >> parsers::number
                                   >> ignore(*parsers::space);
                auto result = double{};
                if (p(arg.GetView(row), result)) {
                  check(b.Append(result));
                  continue;
                }
                if (not failed) {
                  failed = std::string{arg.GetView(row)};
                }
                check(b.AppendNull());
              }
              return finish(b);
            },
            [&](const auto&) -> std::shared_ptr<arrow::DoubleArray> {
              diagnostic::warning("expected `number` or `string`, got `{}`",
                                  name(), value.type.kind())
                .primary(expr)
                .emit(ctx);
              auto b = arrow::DoubleBuilder{tenzir::arrow_memory_pool()};
              check(b.AppendNulls(value.length()));
              return finish(b);
            },
            };
          return series{double_type{}, match(*value.array, f)};
        });
        if (failed) {
          diagnostic::warning("failed to parse string")
            .primary(expr)
            .note(fmt::format("tried to convert: {}", *failed))
            .emit(ctx);
        }
        return result;
      });
  }
};

} // namespace

} // namespace tenzir::plugins::float_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::float_::float_)
