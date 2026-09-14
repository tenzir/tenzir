//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <arrow/type.h>

namespace tenzir::plugins::unflatten {

namespace {

class plugin final : public virtual function_plugin {
public:
  auto name() const -> std::string override {
    return "unflatten";
  }

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_function(function_invocation inv, session ctx) const
    -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    auto sep = Option<std::string>{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .positional("separator", sep)
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr), sep = std::move(sep)](evaluator eval, session) {
        return map_series(eval(expr), [&](series s) {
          auto unflattened = tenzir::unflatten(s.array, sep.value_or("."));
          auto schema = type::from_arrow(*unflattened->type());
          return series{type{s.type.name(), schema}, unflattened};
        });
      });
  }
};

} // namespace

} // namespace tenzir::plugins::unflatten

TENZIR_REGISTER_PLUGIN(tenzir::plugins::unflatten::plugin)
