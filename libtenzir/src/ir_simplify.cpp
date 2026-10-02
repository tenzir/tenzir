//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concepts.hpp"
#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/const_eval.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/instantiate_ctx.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"

#include <algorithm>
#include <utility>
#include <vector>

namespace tenzir {

namespace {

/// What the consumer of a value distinguishes.
enum class Context {
  /// The exact value.
  value,
  /// Whether the value is `true`, `false`, or neither. The logical operators
  /// treat `null` and values of other types alike, and so does `where`, which
  /// keeps an event only if its predicate is `true` and warns if it is
  /// neither `true` nor `false`.
  logic,
  /// Whether the value is `true`, which is all that the condition of an `if`
  /// looks at.
  condition,
};

auto as_bool(ast::expression const& expr) -> Option<bool> {
  auto const* constant = try_as<ast::constant>(expr);
  if (not constant) {
    return None{};
  }
  if (auto const* value = try_as<bool>(constant->value)) {
    return *value;
  }
  return None{};
}

auto is_null(ast::expression const& expr) -> bool {
  auto const* constant = try_as<ast::constant>(expr);
  return constant and is<caf::none_t>(constant->value);
}

/// Returns whether evaluating `expr` performs an operation, as opposed to
/// building a record or list from its operands.
auto is_operation(ast::expression const& expr) -> bool {
  return is<ast::binary_expr>(expr) or is<ast::unary_expr>(expr)
         or is<ast::function_call>(expr) or is<ast::field_access>(expr)
         or is<ast::index_expr>(expr) or is<ast::format_expr>(expr);
}

/// Returns whether the evaluator accepts `x` when the pipeline starts. It
/// instantiates every function call up front, even in operands that it never
/// evaluates, and a call that it rejects, such as `int(x, base=3)`, fails the
/// pipeline. So a predicate may omit `x` where the runtime never evaluates it
/// only if the evaluator accepts `x`.
auto instantiates(ast::expression const& x, registry const& reg) -> bool {
  if (is<ast::constant>(x)) {
    return true;
  }
  auto dh = collecting_diagnostic_handler{};
  auto evaluator = nova::Evaluator::make(x, nova::InstantiateCtx{dh, reg});
  return evaluator and dh.empty();
}

/// Returns whether the evaluator accepts a constant with the given value. It
/// fails the pipeline for some types, such as a secret that a user-defined
/// operator receives as an argument, even where it evaluates no rows.
auto is_evaluable(auto const& value) -> bool {
  return tenzir::match(
    value,
    [](record const& x) {
      return std::ranges::all_of(x, [](auto const& field) {
        return is_evaluable(field.second);
      });
    },
    [](list const& x) {
      return std::ranges::all_of(x, [](data const& item) {
        return is_evaluable(item);
      });
    },
    []<class T>(T const&) {
      return not concepts::one_of<T, pattern, enumeration, map, secret>;
    });
}

/// What the simplifier learns about an expression while simplifying it.
struct Properties {
  /// Whether the expression may fold into a constant. This requires that it
  /// has the same value for every event, so that it reads neither the event
  /// nor a variable and is deterministic. It also requires that it evaluates
  /// without diagnostics, which an operation that failed to fold does not.
  bool constant = false;
  /// Whether every constant in the expression is evaluable as for
  /// `is_evaluable`.
  bool evaluable = false;
  /// Whether evaluating the expression never fails the pipeline, so that a
  /// predicate may omit it although the runtime evaluates it. This requires
  /// that it is evaluable and calls no function, because a function can fail
  /// depending on its input, such as `slice(x, stride=-1)` for a string.
  bool removable = false;
  /// Whether the evaluator rejects the expression as for `instantiates`. The
  /// simplifier does not always know whether the evaluator accepts an
  /// expression, but it remembers when it does not, so that it never checks
  /// an expression that contains a rejected one.
  bool rejected = false;

  friend auto operator&(Properties x, Properties y) -> Properties {
    return {
      .constant = x.constant and y.constant,
      .evaluable = x.evaluable and y.evaluable,
      .removable = x.removable and y.removable,
      .rejected = x.rejected or y.rejected,
    };
  }

  auto operator&=(Properties other) -> Properties& {
    return *this = *this & other;
  }
};

/// The properties of an expression without operands that reads the event.
constexpr auto event_read = Properties{
  .constant = false,
  .evaluable = true,
  .removable = true,
};

/// The neutral element of `Properties::operator&`.
constexpr auto no_operands = Properties{
  .constant = true,
  .evaluable = true,
  .removable = true,
};

/// The result of simplifying the operands of an expression.
struct Simplified {
  /// The expression that replaces the simplified one, if any.
  Option<ast::expression> replacement = None{};
  /// The properties of the simplified expression, or of its replacement.
  Properties properties;
};

class Simplifier {
public:
  explicit Simplifier(registry const& reg) : reg_{reg} {
  }

  /// Simplifies `x` in place and returns its properties. They describe the
  /// result even if folding it failed, so that the enclosing expression does
  /// not rescan it.
  auto simplify(ast::expression& x, Context context) -> Properties {
    auto result = x.match(
      [&](ast::binary_expr& y) {
        return simplify_binary(y, context);
      },
      [&](ast::unary_expr& y) {
        return simplify_unary(y, context);
      },
      [&](auto& y) {
        return Simplified{.properties = simplify_operands(y)};
      });
    if (result.replacement) {
      x = std::move(*result.replacement);
    } else if (result.properties.constant) {
      result.properties = fold(x, result.properties);
    }
    // A condition silently treats `null` as `false`.
    if (context == Context::condition and is_null(x)) {
      x = ast::constant{false, x.get_location()};
    }
    return result.properties;
  }

  /// Returns whether a predicate may omit `x`, which has the given properties,
  /// where the runtime skips it. The evaluator must accept `x` as for
  /// `accepts`. The runtime may also evaluate `x` for no rows, as it does for
  /// the branch of an `if` that the condition skips, which fails for a
  /// constant that is not evaluable.
  auto is_skippable(ast::expression const& x, Properties& properties) const
    -> bool {
    return properties.evaluable and accepts(x, properties);
  }

private:
  /// Returns whether the evaluator accepts `x`, which has the given
  /// properties, as for `instantiates`. Records in `properties` if it does
  /// not.
  auto accepts(ast::expression const& x, Properties& properties) const -> bool {
    if (properties.rejected) {
      return false;
    }
    if (instantiates(x, reg_)) {
      return true;
    }
    properties.rejected = true;
    return false;
  }

  auto simplify_binary(ast::binary_expr& x, Context context) -> Simplified {
    switch (x.op) {
      case ast::binary_op::and_:
      case ast::binary_op::or_:
        return simplify_and_or(x, context);
      case ast::binary_op::if_:
        return simplify_if(x, nullptr, context);
      case ast::binary_op::else_: {
        // The parser represents `a if b else c` as `(a if b) else c`, which
        // does not evaluate `c` when `b` is `true` and `a` is `null`.
        auto* condition = try_as<ast::binary_expr>(x.left);
        if (condition and condition->op == ast::binary_op::if_) {
          return simplify_if(*condition, &x.right, context);
        }
        // The fallback applies only if the value is `null`, so its exact value
        // matters.
        auto const left = simplify(x.left, Context::value);
        auto right = simplify(x.right, context);
        if (is_null(x.left)) {
          return {.replacement = std::move(x.right), .properties = right};
        }
        if (is<ast::constant>(x.left) and is_skippable(x.right, right)) {
          return {.replacement = std::move(x.left), .properties = left};
        }
        return {.properties = left & right};
      }
      case ast::binary_op::add:
      case ast::binary_op::sub:
      case ast::binary_op::mul:
      case ast::binary_op::div:
      case ast::binary_op::eq:
      case ast::binary_op::neq:
      case ast::binary_op::gt:
      case ast::binary_op::geq:
      case ast::binary_op::lt:
      case ast::binary_op::leq:
      case ast::binary_op::in: {
        auto const left = simplify(x.left, Context::value);
        auto const right = simplify(x.right, Context::value);
        return {.properties = left & right};
      }
    }
    TENZIR_UNREACHABLE();
  }

  /// Applies three-valued logic, in which values other than `true` and `false`
  /// act like `null`: `false and x` and `x and false` are `false`, `true or x`
  /// and `x or true` are `true`, and the remaining combinations with `null` are
  /// `null`.
  auto simplify_and_or(ast::binary_expr& x, Context context) -> Simplified {
    // The operands of a condition matter only if they are `true`: `a and b` is
    // `true` if and only if both are, and `a or b` if and only if either is.
    auto const operands
      = context == Context::condition ? Context::condition : Context::logic;
    auto const left_properties = simplify(x.left, operands);
    auto right_properties = simplify(x.right, operands);
    auto const unchanged = [&] {
      return Simplified{.properties = left_properties & right_properties};
    };
    // `false` decides `and`, and `true` decides `or`.
    auto const decisive = x.op == ast::binary_op::or_;
    auto const left = as_bool(x.left);
    auto const right = as_bool(x.right);
    if (left and *left == decisive) {
      // The runtime does not evaluate the right operand, not even for no rows.
      if (accepts(x.right, right_properties)) {
        return {
          .replacement = std::move(x.left),
          .properties = left_properties,
        };
      }
      return unchanged();
    }
    if (right and *right == decisive) {
      if (left_properties.removable) {
        return {
          .replacement = std::move(x.right),
          .properties = right_properties,
        };
      }
      return unchanged();
    }
    // `true and x` is `x` only if `x` is a `bool` or `null`.
    if (context == Context::value) {
      return unchanged();
    }
    if (left) {
      return {
        .replacement = std::move(x.right),
        .properties = right_properties,
      };
    }
    if (right) {
      return {
        .replacement = std::move(x.left),
        .properties = left_properties,
      };
    }
    return unchanged();
  }

  /// Simplifies `x.left if x.right else fallback`, where a missing fallback
  /// reads as `null`.
  auto simplify_if(ast::binary_expr& x, ast::expression* fallback,
                   Context context) -> Simplified {
    TENZIR_ASSERT(x.op == ast::binary_op::if_);
    auto const condition_properties = simplify(x.right, Context::condition);
    auto then_properties = simplify(x.left, context);
    auto fallback_properties
      = fallback ? simplify(*fallback, context) : no_operands;
    auto const unchanged = [&] {
      return Simplified{
        .properties
        = condition_properties & then_properties & fallback_properties,
      };
    };
    auto const condition = as_bool(x.right);
    if (not condition) {
      return unchanged();
    }
    // The runtime evaluates the branch that the condition skips for no rows.
    if (*condition) {
      if (fallback and not is_skippable(*fallback, fallback_properties)) {
        return unchanged();
      }
      return {
        .replacement = std::move(x.left),
        .properties = then_properties,
      };
    }
    if (not is_skippable(x.left, then_properties)) {
      return unchanged();
    }
    if (fallback) {
      return {
        .replacement = std::move(*fallback),
        .properties = fallback_properties,
      };
    }
    return {
      .replacement = ast::constant{caf::none, x.get_location()},
      .properties = no_operands,
    };
  }

  auto simplify_unary(ast::unary_expr& x, Context context) -> Simplified {
    if (x.op != ast::unary_op::not_) {
      auto properties = simplify(x.expr, Context::value);
      // Moving a field removes it from the event.
      properties.constant = properties.constant and x.op != ast::unary_op::move;
      return {.properties = properties};
    }
    // `not not x` is `x` only if `x` is a `bool` or `null`. Simplifying `x`
    // directly visits it only once.
    auto* inner = try_as<ast::unary_expr>(x.expr);
    if (context != Context::value and inner
        and inner->op == ast::unary_op::not_) {
      auto result = std::move(inner->expr);
      auto const properties = simplify(result, context);
      return {.replacement = std::move(result), .properties = properties};
    }
    auto const properties = simplify(x.expr, Context::logic);
    // Simplifying the operand can result in a double negation as well, as for
    // `not (true and not x)`.
    inner = try_as<ast::unary_expr>(x.expr);
    if (context == Context::value or not inner
        or inner->op != ast::unary_op::not_) {
      return {.properties = properties};
    }
    auto result = std::move(inner->expr);
    // The simplifier already simplified `result` for logic, and the properties
    // of `not result` hold for `result`. Only a condition allows simplifying it
    // further, which takes another pass over `result`.
    if (context == Context::logic) {
      return {.replacement = std::move(result), .properties = properties};
    }
    auto const result_properties = simplify(result, context);
    return {
      .replacement = std::move(result),
      .properties = result_properties,
    };
  }

  auto simplify_operands(ast::function_call& x) -> Properties {
    // A function may require syntax rather than a value for an argument, such
    // as a field for `drop_null_fields(x, a)`, a lambda for `count_if`, or a
    // constant. Simplifying an argument could then make a rejected call valid,
    // as with `count_if(xs, (x => true) if true)`. Simplification preserves
    // the syntax that a valid call requires. A rejected call fails the
    // pipeline, so an expression that contains it never folds and stays.
    //
    // Instantiating a call prepares its argument expressions and lambda
    // bodies, and evaluates its constant arguments. So the evaluator accepts
    // every call in the arguments of a call that it accepts, and checking
    // them again would take quadratic time for nested calls.
    if (not in_accepted_call_ and not instantiates(x, reg_)) {
      return {.rejected = true};
    }
    auto properties = Properties{
      .constant = x.fn.ref.resolved() and reg_.get(x).is_deterministic(),
      .evaluable = true,
      // The function may fail depending on its input.
      .removable = false,
    };
    auto const in_accepted_call = std::exchange(in_accepted_call_, true);
    for (auto& arg : x.args) {
      properties &= simplify(arg, Context::value);
    }
    in_accepted_call_ = in_accepted_call;
    return properties;
  }

  auto simplify_operands(ast::assignment& x) -> Properties {
    // The left side of a named argument is a label. The assignment occurs
    // only as an argument of a function call, which is never removable.
    auto properties = simplify(x.right, Context::value);
    properties.removable = false;
    return properties;
  }

  auto simplify_operands(ast::field_access& x) -> Properties {
    return simplify(x.left, Context::value);
  }

  auto simplify_operands(ast::index_expr& x) -> Properties {
    auto const expr = simplify(x.expr, Context::value);
    auto const index = simplify(x.index, Context::value);
    return expr & index;
  }

  auto simplify_operands(ast::record& x) -> Properties {
    auto properties = no_operands;
    auto spreads = size_t{0};
    for (auto& item : x.items) {
      properties &= item.match(
        [&](ast::record::field& field) {
          return simplify(field.expr, Context::value);
        },
        [&](ast::spread& spread) {
          ++spreads;
          return simplify(spread.expr, Context::value);
        });
    }
    // The evaluator rejects a record literal with several spreads.
    if (spreads > 1) {
      properties.removable = false;
    }
    return properties;
  }

  auto simplify_operands(ast::list& x) -> Properties {
    auto properties = no_operands;
    for (auto& item : x.items) {
      properties &= item.match(
        [&](ast::expression& expr) {
          return simplify(expr, Context::value);
        },
        [&](ast::spread& spread) {
          return simplify(spread.expr, Context::value);
        });
    }
    return properties;
  }

  auto simplify_operands(ast::lambda_expr& x) -> Properties {
    // A lambda is not a value that we could fold.
    auto const body = simplify(x.body, Context::value);
    return {
      .constant = false,
      .evaluable = body.evaluable,
      .removable = false,
      .rejected = body.rejected,
    };
  }

  auto simplify_operands(ast::unpack& x) -> Properties {
    auto const expr = simplify(x.expr, Context::value);
    return {
      .constant = false,
      .evaluable = expr.evaluable,
      .removable = false,
      .rejected = expr.rejected,
    };
  }

  auto simplify_operands(ast::format_expr& x) -> Properties {
    auto properties = no_operands;
    for (auto& segment : x.segments) {
      if (auto* replacement = try_as<ast::format_expr::replacement>(segment)) {
        properties &= simplify(replacement->expr, Context::value);
      }
    }
    return properties;
  }

  auto simplify_operands(ast::constant& x) -> Properties {
    auto const evaluable = is_evaluable(x.value);
    return {.constant = true, .evaluable = evaluable, .removable = evaluable};
  }

  auto simplify_operands(ast::this_&) -> Properties {
    return event_read;
  }

  auto simplify_operands(ast::root_field&) -> Properties {
    return event_read;
  }

  auto simplify_operands(ast::meta&) -> Properties {
    return event_read;
  }

  auto simplify_operands(ast::pkg_dollar_var& x) -> Properties {
    // The evaluator evaluates the cached value like a constant.
    return {
      .constant = false,
      .evaluable = x.value and is_evaluable(*x.value),
      .removable = false,
    };
  }

  template <class T>
  auto simplify_operands(T&) -> Properties {
    // The remaining expressions have no operands that we could simplify, and
    // we know nothing about them.
    return {};
  }

  /// Replaces an operation by its value if possible, and returns the
  /// properties of the result. Requires that `x`, which has the given
  /// properties, is constant.
  auto fold(ast::expression& x, Properties properties) -> Properties {
    if (not is_operation(x)) {
      return properties;
    }
    // Folding would drop the diagnostics that the evaluation emits for every
    // batch at runtime.
    auto dh = collecting_diagnostic_handler{};
    auto value = nova::const_eval(x, nova::InstantiateCtx{dh, reg_});
    if (not value or not dh.empty()) {
      // An operation that evaluates `x` fails to fold as well, so it need not
      // evaluate `x` again. Otherwise, folding would take quadratic time for
      // nested operations.
      properties.constant = false;
      return properties;
    }
    auto legacy = nova::materialize_legacy(*value);
    if (not is_evaluable(legacy)) {
      return properties;
    }
    x = ast::constant::make(located{std::move(legacy), x.get_location()});
    return no_operands;
  }

  registry const& reg_;
  /// Whether the expression being simplified is in the arguments of a call
  /// that the evaluator accepts.
  bool in_accepted_call_ = false;
};

/// Counts the expression nodes of an expression.
class SizeCounter final : public ast::visitor<SizeCounter> {
public:
  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::expression& x) -> void {
    ++size;
    enter(x);
  }

  size_t size = 0;
};

} // namespace

auto ir::simplify_predicate(ast::expression predicate) -> ast::expression {
  auto reg = global_registry();
  auto simplifier = Simplifier{*reg};
  simplifier.simplify(predicate, Context::logic);
  return predicate;
}

auto ir::simplify_filter(OptimizeFilter filter) -> OptimizeFilter {
  auto reg = global_registry();
  auto simplifier = Simplifier{*reg};
  auto simplified = OptimizeFilter{};
  auto properties = std::vector<Properties>{};
  for (auto& predicate : filter) {
    auto const predicate_properties
      = simplifier.simplify(predicate, Context::logic);
    if (as_bool(predicate) != true) {
      simplified.push_back(std::move(predicate));
      properties.push_back(predicate_properties);
    }
  }
  auto const never
    = std::ranges::find_if(simplified, [](ast::expression const& x) {
        return as_bool(x) == false;
      });
  if (never == simplified.end()) {
    return simplified;
  }
  // No event passes, so the other predicates matter only if they could fail
  // the pipeline. The predicates behind `false` never see an event.
  auto const never_index = never - simplified.begin();
  auto result = OptimizeFilter{};
  for (auto i = ptrdiff_t{0}; i < std::ssize(simplified); ++i) {
    if (i != never_index) {
      auto const omittable
        = i < never_index
            ? properties[i].removable
            : simplifier.is_skippable(simplified[i], properties[i]);
      if (omittable) {
        continue;
      }
    }
    result.push_back(std::move(simplified[i]));
  }
  return result;
}

auto ir::expression_size(ast::expression const& expr) -> size_t {
  auto counter = SizeCounter{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  counter.visit(const_cast<ast::expression&>(expr));
  return counter.size;
}

} // namespace tenzir
