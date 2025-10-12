//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/subnet.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/subnet.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/type.hpp>

namespace tenzir::plugins::subnet {

namespace {

class subnet final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.subnet";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function("subnet")
          .positional("x", expr, "string")
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) {
        return map_series(eval(expr), [&](series arg) {
          auto f = detail::overload{
            [](const arrow::NullArray& arg) {
              return series::null(subnet_type{}, arg.length());
            },
            [](const arrow::StringArray& arg) {
              auto b
                = subnet_type::make_arrow_builder(arrow_memory_pool());
              check(b->Reserve(arg.length()));
              for (auto i = 0; i < arg.length(); ++i) {
                if (arg.IsNull(i)) {
                  check(b->AppendNull());
                  continue;
                }
                auto result = tenzir::subnet{};
                if (parsers::net(arg.GetView(i), result)) {
                  check(append_builder(subnet_type{}, *b, result));
                } else {
                  // TODO: ?
                  check(b->AppendNull());
                }
              }
              return series{subnet_type{}, check(b->Finish())};
            },
            [&](const subnet_type::array_type&) {
              return arg;
            },
            [&](const auto&) {
              diagnostic::warning("`subnet` expected `string`, but got `{}`",
                                  arg.type.kind())
                .primary(expr)
                .emit(ctx);
              return series::null(subnet_type{}, arg.length());
            },
          };
          return match(*arg.array, f);
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::subnet

TENZIR_REGISTER_PLUGIN(tenzir::plugins::subnet::subnet)
