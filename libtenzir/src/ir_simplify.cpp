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
#include "tenzir/nova/function_plugin.hpp"
#include "tenzir/nova/instantiate_ctx.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"

#include <algorithm>
#include <ranges>
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

/// Returns whether `expr` is a constant, or a list or record literal that holds
/// only such values. The evaluator repeats the value of such a literal for all
/// rows instead of building it row by row.
auto is_constant_literal(ast::expression const& expr) -> bool {
  return expr.match(
    [](ast::constant const&) {
      return true;
    },
    [](ast::list const& x) {
      return std::ranges::all_of(x.items, [](auto const& item) {
        auto const* element = try_as<ast::expression>(item);
        return element and is_constant_literal(*element);
      });
    },
    [](ast::record const& x) {
      return std::ranges::all_of(x.items, [](auto const& item) {
        auto const* field = try_as<ast::record::field>(item);
        return field and is_constant_literal(field->expr);
      });
    },
    [](auto const&) {
      return false;
    });
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

/// Returns whether evaluating `x` never fails the pipeline, so that a
/// predicate may omit `x` although the runtime evaluates it. A function can
/// fail depending on its input, such as `slice(x, stride=-1)` for a string,
/// so `x` must not call any.
auto is_removable(ast::expression const& x) -> bool {
  auto const is_removable_item = [](auto const& item) {
    return tenzir::match(
      item,
      [](ast::expression const& expr) {
        return is_removable(expr);
      },
      [](ast::record::field const& field) {
        return is_removable(field.expr);
      },
      [](ast::spread const& spread) {
        return is_removable(spread.expr);
      });
  };
  return x.match(
    [](ast::constant const& y) {
      return is_evaluable(y.value);
    },
    [](ast::this_ const&) {
      return true;
    },
    [](ast::root_field const&) {
      return true;
    },
    [](ast::meta const&) {
      return true;
    },
    [](ast::field_access const& y) {
      return is_removable(y.left);
    },
    [](ast::index_expr const& y) {
      return is_removable(y.expr) and is_removable(y.index);
    },
    [](ast::binary_expr const& y) {
      return is_removable(y.left) and is_removable(y.right);
    },
    [](ast::unary_expr const& y) {
      return is_removable(y.expr);
    },
    [&](ast::record const& y) {
      // The evaluator rejects a record literal with several spreads.
      auto const spreads = std::ranges::count_if(y.items, [](auto const& item) {
        return is<ast::spread>(item);
      });
      return spreads <= 1 and std::ranges::all_of(y.items, is_removable_item);
    },
    [&](ast::list const& y) {
      return std::ranges::all_of(y.items, is_removable_item);
    },
    [](ast::format_expr const& y) {
      return std::ranges::all_of(
        y.segments, [](ast::format_expr::segment const& segment) {
          auto const* replacement
            = try_as<ast::format_expr::replacement>(segment);
          return not replacement or is_removable(replacement->expr);
        });
    },
    [](auto const&) {
      return false;
    });
}

/// Detects a `move`, which removes the field that it reads.
class MoveFinder final : public ast::visitor<MoveFinder> {
public:
  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::unary_expr& x) -> void {
    found = found or x.op == ast::unary_op::move;
    enter(x);
  }

  bool found = false;
};

/// Returns whether `x` contains a `move`.
auto moves(ast::expression const& x) -> bool {
  auto finder = MoveFinder{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  finder.visit(const_cast<ast::expression&>(x));
  return finder.found;
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
    if (context != Context::value and rewrite_call_access(x, context)) {
      return simplify(x, context);
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
      // If both branches are the same constant, the condition matters only if
      // evaluating it could fail.
      auto const left = as_bool(x.left);
      if (left and fallback and as_bool(*fallback) == left
          and condition_properties.removable) {
        return {
          .replacement = std::move(x.left),
          .properties = then_properties,
        };
      }
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

  /// An access to a field of the result of a function call whose function may
  /// describe the field over the call's arguments.
  struct CallAccess {
    ast::expression const& base;
    ast::function_call const& call;
    nova::FunctionPlugin const& plugin;
    std::vector<ast::field_path::segment> path;

    /// Whether the evaluator accepts the call. Checked only once a function
    /// describes the access, since instantiating a call is costly.
    auto instantiates(registry const& reg) const -> bool {
      return tenzir::instantiates(base, reg);
    }
  };

  /// Returns `x` as a call access that is eligible for a rewrite: The call
  /// must be deterministic, and its arguments must neither fail nor move
  /// fields, so that a rewrite may evaluate them more often, or not at all.
  /// The path may be empty, for a call whose value the function describes.
  auto as_call_access(ast::expression const& x) const -> Option<CallAccess> {
    auto path = std::vector<ast::field_path::segment>{};
    auto const* base = &x;
    while (auto const* access = try_as<ast::field_access>(*base)) {
      path.push_back({access->name, access->has_question_mark});
      base = &access->left;
    }
    auto const* call = try_as<ast::function_call>(*base);
    if (not call or not call->fn.ref.resolved()) {
      return None{};
    }
    auto const* plugin
      = dynamic_cast<nova::FunctionPlugin const*>(&reg_.get(*call));
    if (not plugin) {
      return None{};
    }
    for (auto const& arg : call->args) {
      auto const* named = try_as<ast::assignment>(arg);
      auto const& value = named ? named->right : arg;
      if (not is_removable(value) or moves(value)) {
        return None{};
      }
    }
    if (not base->is_deterministic(reg_)) {
      return None{};
    }
    std::ranges::reverse(path);
    return CallAccess{*base, *call, *plugin, std::move(path)};
  }

  /// Returns whether `x` is a constant or a list of constants.
  static auto is_constant_list(ast::expression const& x) -> bool {
    if (auto const* constant = try_as<ast::constant>(x)) {
      return is<list>(constant->value);
    }
    auto const* items = try_as<ast::list>(x);
    return items and std::ranges::all_of(items->items, [](auto const& item) {
             auto const* expr = try_as<ast::expression>(item);
             return expr and is<ast::constant>(*expr);
           });
  }

  /// Rewrites the predicate `x` if it reads a field of a call's result as an
  /// operand, and the function can describe the field over the call's
  /// arguments, as in `FunctionPlugin::rewrite_field_access`. The operand must
  /// be one that the predicate always evaluates: `x` itself, or a side of a
  /// comparison. The rewrite is local to `x`, so it evaluates the arguments of
  /// the call only where `x` does.
  auto rewrite_call_access(ast::expression& x, Context context) -> bool {
    if (lambda_depth_ > 0 or call_depth_ > 0) {
      return false;
    }
    auto* binary = try_as<ast::binary_expr>(x);
    if (not binary) {
      auto access = as_call_access(x);
      return access and rewrite_value(x, *access, x, context);
    }
    switch (binary->op) {
      case ast::binary_op::eq:
      case ast::binary_op::neq:
      case ast::binary_op::gt:
      case ast::binary_op::geq:
      case ast::binary_op::lt:
      case ast::binary_op::leq:
      case ast::binary_op::in:
        break;
      default:
        return false;
    }
    auto const comparison
      = binary->op == ast::binary_op::eq or binary->op == ast::binary_op::neq;
    for (auto* side : {&binary->left, &binary->right}) {
      // `in` is not commutative, and only a field on its left compares.
      if (side == &binary->right and binary->op == ast::binary_op::in) {
        break;
      }
      auto access = as_call_access(*side);
      if (not access) {
        continue;
      }
      auto const& other = side == &binary->left ? binary->right : binary->left;
      auto const comparable
        = (comparison and is<ast::constant>(other))
          or (binary->op == ast::binary_op::in and is_constant_list(other));
      if (comparable) {
        auto predicate = access->plugin.rewrite_field_comparison(
          access->call, access->path, binary->op, other, {.reg = reg_});
        if (predicate and access->instantiates(reg_)
            and commit(x, std::move(*predicate))) {
          return true;
        }
      }
      if (rewrite_value(x, *access, *side, context)) {
        return true;
      }
    }
    return false;
  }

  /// Replaces `operand`, which is `x` or one of its operands and reads the
  /// field of `access`, by the description of the field. The cases of the
  /// description become disjuncts: for a predicate `P[operand]` and cases
  /// `(g1, v1), …, (gn, vn)`, the result is `(g1 and P[v1]) or … or (gn and
  /// P[vn]) or (not (g1 or … or gn) and P[null])`. As the guards are `bool`s
  /// that are never `null` and at most one of them is `true`, this has the
  /// same value as `P[operand]` in three-valued logic. A disjunct whose
  /// predicate simplifies to `false` is omitted.
  auto rewrite_value(ast::expression& x, CallAccess const& access,
                     ast::expression& operand, Context context) -> bool {
    auto cases = access.plugin.rewrite_field_access(access.call, access.path,
                                                    {.reg = reg_});
    if (not cases or not access.instantiates(reg_)) {
      return false;
    }
    // Reserve the budget for the cases before simplifying the predicates over
    // them, which may rewrite further accesses, so that the recursion ends
    // even if a description reads its own call again.
    auto reserved = size_t{0};
    for (auto const& [guard, value] : *cases) {
      reserved += ir::expression_size(guard) + ir::expression_size(value);
    }
    if (reserved > rewrite_budget_) {
      return false;
    }
    rewrite_budget_ -= reserved;
    auto const source = operand.get_location();
    auto const instantiate = [&](ast::expression value) {
      auto original = std::exchange(operand, std::move(value));
      auto result = x;
      operand = std::move(original);
      simplify(result, context);
      return result;
    };
    auto const disjoin = [&](Option<ast::expression> lhs, ast::expression rhs) {
      if (not lhs) {
        return rhs;
      }
      return ast::expression{
        ast::binary_expr{std::move(*lhs), ast::binary_op::or_, std::move(rhs)}};
    };
    auto const conjoin = [](ast::expression guard, ast::expression predicate) {
      if (as_bool(guard) == true) {
        return predicate;
      }
      return ast::expression{ast::binary_expr{
        std::move(guard), ast::binary_op::and_, std::move(predicate)}};
    };
    auto result = Option<ast::expression>{};
    auto any = Option<ast::expression>{};
    for (auto& [guard, value] : *cases) {
      any = disjoin(std::move(any), guard);
      auto predicate = instantiate(std::move(value));
      if (as_bool(predicate) == false) {
        continue;
      }
      result = disjoin(std::move(result),
                       conjoin(std::move(guard), std::move(predicate)));
    }
    auto fallback = instantiate(ast::constant{caf::none, source});
    if (as_bool(fallback) != false) {
      auto none = any ? ast::expression{ast::unary_expr{
                          located{ast::unary_op::not_, source},
                          std::move(*any),
                        }}
                      : ast::expression{ast::constant{true, source}};
      result = disjoin(std::move(result),
                       conjoin(std::move(none), std::move(fallback)));
    }
    return commit(x, result ? std::move(*result)
                            : ast::expression{ast::constant{false, source}});
  }

  /// Replaces `x` by `replacement` if the rewrite budget suffices.
  auto commit(ast::expression& x, ast::expression replacement) -> bool {
    auto const size = ir::expression_size(replacement);
    if (size > rewrite_budget_) {
      return false;
    }
    rewrite_budget_ -= size;
    x = std::move(replacement);
    return true;
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
    ++call_depth_;
    for (auto& arg : x.args) {
      // A named argument is an assignment, not an expression that assigns.
      if (auto* named = try_as<ast::assignment>(arg)) {
        properties &= simplify(named->right, Context::value);
      } else {
        properties &= simplify(arg, Context::value);
      }
    }
    --call_depth_;
    in_accepted_call_ = in_accepted_call;
    return properties;
  }

  auto simplify_operands(ast::assignment& x) -> Properties {
    // An assignment in place of a value only warns at runtime. Named
    // arguments are handled separately while simplifying a function call.
    auto properties = simplify(x.right, Context::value);
    properties.constant = false;
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
    ++lambda_depth_;
    auto const body = simplify(x.body, Context::value);
    --lambda_depth_;
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
  /// The number of function calls and lambdas whose arguments or body are
  /// being simplified. Rewrites of call accesses apply only outside of them.
  size_t call_depth_ = 0;
  size_t lambda_depth_ = 0;
  /// The number of expression nodes that rewrites of call accesses may still
  /// produce.
  size_t rewrite_budget_ = ir::max_rewritten_predicate_size;
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

/// Finds the parts of an expression whose evaluation costs more than reading
/// fields and combining them with operators.
class CostFinder final : public ast::visitor<CostFinder> {
public:
  explicit CostFinder(registry const& reg) : reg_{reg} {
  }

  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::function_call& x) -> void {
    if (not x.fn.ref.resolved() or not reg_.get(x).is_cheap()) {
      ++expensive;
    }
    enter(x);
  }

  auto visit(ast::lambda_expr& x) -> void {
    ++expensive;
    enter(x);
  }

  auto visit(ast::pipeline_expr&) -> void {
    ++expensive;
  }

  /// A format string stringifies its values, which may be nested, and builds a
  /// new string.
  auto visit(ast::format_expr& x) -> void {
    ++expensive;
    enter(x);
  }

  /// The number of expensive parts, including those nested in others.
  size_t expensive = 0;

private:
  registry const& reg_;
};

/// Replaces package `let` references by their cached values where the
/// evaluator accepts them. The simplifier keeps the references, but a
/// condition that only reads them is constant.
class PackageLetFolder final : public ast::visitor<PackageLetFolder> {
public:
  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::expression& x) -> void {
    auto* var = try_as<ast::pkg_dollar_var>(x);
    if (not var) {
      enter(x);
      return;
    }
    if (var->value and is_evaluable(*var->value)) {
      x = ast::constant::make(located{*var->value, var->get_location()});
    }
  }
};

} // namespace

auto ir::simplify_predicate(ast::expression predicate, registry const& reg)
  -> ast::expression {
  auto simplifier = Simplifier{reg};
  simplifier.simplify(predicate, Context::logic);
  return predicate;
}

auto ir::simplify_filter(OptimizeFilter filter, registry const& reg)
  -> OptimizeFilter {
  auto simplifier = Simplifier{reg};
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

auto ir::constant_condition(ast::expression const& condition,
                            registry const& reg) -> Option<bool> {
  auto simplifier = Simplifier{reg};
  auto simplified = condition;
  PackageLetFolder{}.visit(simplified);
  simplifier.simplify(simplified, Context::condition);
  return as_bool(simplified);
}

auto ir::expression_size(ast::expression const& expr) -> size_t {
  auto counter = SizeCounter{};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  counter.visit(const_cast<ast::expression&>(expr));
  return counter.size;
}

auto ir::is_plain(ast::expression const& expr) -> bool {
  return expr.match(
    [](ast::constant const&) {
      return true;
    },
    [&](ast::list const&) {
      return is_constant_literal(expr);
    },
    [&](ast::record const&) {
      return is_constant_literal(expr);
    },
    [](ast::meta const&) {
      return true;
    },
    [](ast::this_ const&) {
      return true;
    },
    [](ast::root_field const&) {
      return true;
    },
    [](ast::field_access const& x) {
      return is_plain(x.left);
    },
    [](ast::index_expr const& x) {
      return is_plain(x.expr) and is<ast::constant>(x.index);
    },
    [](auto const&) {
      return false;
    });
}

auto ir::count_expensive(ast::expression const& expr, registry const& reg)
  -> size_t {
  auto finder = CostFinder{reg};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  finder.visit(const_cast<ast::expression&>(expr));
  return finder.expensive;
}

auto ir::is_cheap(ast::expression const& expr, registry const& reg) -> bool {
  return count_expensive(expr, reg) == 0;
}

auto ir::is_infallible(ast::expression const& expr) -> bool {
  return is_removable(expr);
}

auto ir::conjoin_filter(OptimizeFilter filter) -> ast::expression {
  if (filter.empty()) {
    return ast::constant{true, location::unknown};
  }
  auto result = std::move(filter.front());
  for (auto& predicate : filter | std::views::drop(1)) {
    // The chain evaluates `predicate` only if `result` is `true`, and so does
    // `predicate if result else false`. `result and predicate` also evaluates
    // it if `result` is `null`, which is harmless only if it cannot fail.
    if (is_removable(predicate)) {
      result = ast::binary_expr{std::move(result), ast::binary_op::and_,
                                std::move(predicate)};
      continue;
    }
    auto const source = predicate.get_location();
    result = ast::binary_expr{
      ast::binary_expr{std::move(predicate), ast::binary_op::if_,
                       std::move(result)},
      ast::binary_op::else_, ast::constant{false, source}};
  }
  return result;
}

} // namespace tenzir
