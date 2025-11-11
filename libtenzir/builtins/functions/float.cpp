//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/plugin.hpp"

namespace tenzir::plugins::float_ {

namespace {

class float_ final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "float";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "number|string")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), this](auto eval, session ctx) {
        return map_series(eval(expr), [&](series value) {
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
                diagnostic::warning("failed to parse string")
                  .primary(expr)
                  .note(fmt::format("tried to convert: {}", arg.GetView(row)))
                  .emit(ctx);
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
      });
  }
};

} // namespace

} // namespace tenzir::plugins::float_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::float_::float_)
