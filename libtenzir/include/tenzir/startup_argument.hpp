// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/nova/argument_conversion.hpp"
#include "tenzir/nova/const_eval.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/try.hpp"

namespace tenzir {

/// An argument recipe evaluated by its operator during startup. Only the
/// unprepared expression is persisted; evaluation never changes the recipe.
template <class T>
class StartupArgument {
public:
  StartupArgument() = default;

  explicit StartupArgument(ast::expression expression)
    : expression_{std::move(expression)} {
  }

  /// Creates a recipe for a constant, e.g., as the default of an argument.
  static auto constant(data value, location source = location::unknown)
    -> StartupArgument {
    return StartupArgument{
      ast::constant::make(located{std::move(value), source})};
  }

  auto get_location() const -> location {
    return expression_.get_location();
  }

  auto expression() const -> ast::expression const& {
    return expression_;
  }

  auto evaluate(OpCtx& ctx) const -> Task<failure_or<T>> {
    CO_TRY(auto value, co_await nova::const_eval(expression_, ctx));
    // `const_eval` already registered the secrets in the value with the
    // censor. Literals promoted to secrets stay printable.
    co_return nova::convert_argument<T>(std::move(value), get_location(),
                                        ctx.dh());
  }

  /// Extracts UTF-8 at an API boundary that consumes a credential as text.
  auto evaluate_string(OpCtx& ctx, std::string_view name) const
    -> Task<failure_or<std::string>>
    requires std::same_as<T, nova::Secret>
  {
    CO_TRY(auto value, co_await evaluate(ctx));
    co_return nova::reveal_string(value, name, get_location(), ctx.dh());
  }

  friend auto inspect(auto& f, StartupArgument& x) -> bool {
    return f.apply(x.expression_);
  }

private:
  ast::expression expression_;
};

template <class T>
inline constexpr bool is_startup_argument = false;

template <class T>
inline constexpr bool is_startup_argument<StartupArgument<T>> = true;

} // namespace tenzir
