//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/ir_lift.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"

#include <caf/binary_serializer.hpp>
#include <caf/byte_buffer.hpp>

#include <algorithm>
#include <ranges>

namespace tenzir {

namespace {

/// Checks whether evaluating an expression again yields the same value.
class RepeatabilityChecker final : public ast::visitor<RepeatabilityChecker> {
public:
  explicit RepeatabilityChecker(registry const& reg) : reg_{reg} {
  }

  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::unary_expr& x) -> void {
    // A `move` removes the field that it reads.
    repeatable = repeatable and x.op != ast::unary_op::move;
    enter(x);
  }

  auto visit(ast::function_call& x) -> void {
    repeatable
      = repeatable and x.fn.ref.resolved() and reg_.get(x).is_deterministic();
    for (auto& arg : x.args) {
      // A named argument is an assignment.
      if (auto* named = try_as<ast::assignment>(arg)) {
        visit(named->right);
      } else {
        visit(arg);
      }
    }
  }

  auto visit(ast::assignment&) -> void {
    // An assignment in place of a value only warns, and the evaluator does
    // not support it inside other expressions.
    repeatable = false;
  }

  auto visit(ast::dollar_var&) -> void {
    // A variable that is still unbound has no value yet.
    repeatable = false;
  }

  auto visit(ast::pipeline_expr&) -> void {
    repeatable = false;
  }

  bool repeatable = true;

private:
  registry const& reg_;
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

/// Returns a key that two expressions share if and only if they are identical,
/// including their locations.
auto make_key(ast::expression const& expr) -> caf::byte_buffer {
  auto buffer = caf::byte_buffer{};
  auto f = caf::binary_serializer{buffer};
  auto copy = expr;
  auto const success = f.apply(copy);
  TENZIR_ASSERT(success);
  return buffer;
}

auto filter_size(ir::OptimizeFilter const& filter) -> size_t {
  auto result = size_t{0};
  for (auto const& predicate : filter) {
    result += ir::expression_size(predicate);
  }
  return result;
}

struct Branch {
  Option<ast::expression> condition;
  ir::OptimizeFilter filter;
  /// The keys of the predicates of `filter`.
  std::vector<caf::byte_buffer> keys;

  /// Returns whether the branch keeps no event.
  auto keeps_none() const -> bool {
    return filter.size() == 1 and as_bool(filter.front()) == false;
  }

  auto same_filter(Branch const& other) const -> bool {
    return keys == other.keys;
  }
};

/// Returns whether `expr` can be `null` only with a warning. A logical
/// operator or `where` warns for other values that are not `bool`.
auto is_strict_bool(ast::expression const& expr) -> bool {
  return expr.match(
    [](ast::constant const& x) {
      return is<bool>(x.value);
    },
    [](ast::binary_expr const& x) {
      switch (x.op) {
        case ast::binary_op::eq:
        case ast::binary_op::neq:
        case ast::binary_op::gt:
        case ast::binary_op::geq:
        case ast::binary_op::lt:
        case ast::binary_op::leq:
        case ast::binary_op::in:
          // A comparison warns for operands that it cannot compare.
          return true;
        case ast::binary_op::and_:
        case ast::binary_op::or_:
          return is_strict_bool(x.left) and is_strict_bool(x.right);
        case ast::binary_op::else_: {
          // The fallback replaces `null`, but not the value of `x if c`.
          auto const* condition = try_as<ast::binary_expr>(x.left);
          if (condition and condition->op == ast::binary_op::if_) {
            return is_strict_bool(condition->left) and is_strict_bool(x.right);
          }
          return is_strict_bool(x.right);
        }
        case ast::binary_op::add:
        case ast::binary_op::sub:
        case ast::binary_op::mul:
        case ast::binary_op::div:
        case ast::binary_op::if_:
          return false;
      }
      TENZIR_UNREACHABLE();
    },
    [](ast::unary_expr const& x) {
      return x.op == ast::unary_op::not_ and is_strict_bool(x.expr);
    },
    [](auto const&) {
      return false;
    });
}

/// Returns a predicate that keeps the events that `condition` routes, which
/// treats `null` as `false` without a warning, just like the routing does.
auto routing_predicate(ast::expression condition) -> ast::expression {
  if (is_strict_bool(condition)) {
    return condition;
  }
  auto const source = condition.get_location();
  return ast::binary_expr{std::move(condition), ast::binary_op::else_,
                          ast::constant{false, source}};
}

auto filter_location(ir::OptimizeFilter const& filter) -> location {
  auto result = location::unknown;
  for (auto const& predicate : filter) {
    result = result.combine(predicate.get_location());
  }
  return result;
}

/// Gives a node that routes events the location of the branch filters in it,
/// so that diagnostics for its value point to the filters rather than to the
/// whole routing operator.
auto relocate(ast::expression& expr, location source) -> void {
  if (not source) {
    return;
  }
  if (auto* binary = try_as<ast::binary_expr>(expr)) {
    binary->location = source;
  }
}

auto make_or(ast::expression left, ast::expression right, location source)
  -> ast::expression {
  auto result = ast::expression{
    ast::binary_expr{std::move(left), ast::binary_op::or_, std::move(right)}};
  relocate(result, source);
  return result;
}

/// Returns `then if condition else otherwise`.
auto make_if_else(ast::expression then, ast::expression condition,
                  ast::expression otherwise, location then_source,
                  location source) -> ast::expression {
  auto inner = ast::expression{ast::binary_expr{
    std::move(then), ast::binary_op::if_, std::move(condition)}};
  relocate(inner, then_source);
  auto result = ast::expression{ast::binary_expr{
    std::move(inner), ast::binary_op::else_, std::move(otherwise)}};
  relocate(result, source);
  return result;
}

} // namespace

auto ir::returns_events(pipeline const& branch) -> Option<bool> {
  auto dh = null_diagnostic_handler{};
  auto output = branch.infer_type(events_element_type(), dh);
  if (not output) {
    return None{};
  }
  return output->is<table_slice>() or output->is<nova::Events>();
}

auto ir::is_repeatable(ast::expression const& expr) -> bool {
  auto const reg = global_registry();
  auto checker = RepeatabilityChecker{*reg};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  checker.visit(const_cast<ast::expression&>(expr));
  return checker.repeatable;
}

auto ir::lift_branch_filters(std::vector<RoutedBranch> routed)
  -> Option<OptimizeFilter> {
  TENZIR_ASSERT(not routed.empty());
  auto largest = size_t{0};
  auto branches = std::vector<Branch>{};
  branches.reserve(routed.size());
  for (auto& branch : routed) {
    auto const last = branches.size() + 1 == routed.size();
    TENZIR_ASSERT(last != branch.condition.has_value());
    largest = std::max(largest, filter_size(branch.filter));
    auto keys = std::vector<caf::byte_buffer>{};
    keys.reserve(branch.filter.size());
    for (auto const& predicate : branch.filter) {
      keys.push_back(make_key(predicate));
    }
    branches.push_back(Branch{
      .condition = std::move(branch.condition),
      .filter = std::move(branch.filter),
      .keys = std::move(keys),
    });
  }
  // A predicate that starts every filter applies to every event before it is
  // routed. One that ends every filter applies to every event that the rest of
  // its branch filter keeps. The filter of a branch that keeps no event
  // evaluates nothing after its routing, so a common suffix need not end it,
  // and a common prefix need not start it if evaluating the prefix for the
  // events of that branch cannot fail.
  auto prefix = OptimizeFilter{};
  auto suffix = OptimizeFilter{};
  {
    auto keeping = std::vector<Branch*>{};
    for (auto& branch : branches) {
      if (not branch.keeps_none()) {
        keeping.push_back(&branch);
      }
    }
    auto const keeps_all = keeping.size() == branches.size();
    auto const common = [&](auto select) {
      // A predicate of a single branch that keeps events stays behind its
      // condition.
      if (keeping.size() < 2 or keeping.front()->keys.empty()) {
        return false;
      }
      auto const& first = select(keeping.front()->keys);
      return std::ranges::all_of(keeping, [&](Branch const* branch) {
        return not branch->keys.empty() and select(branch->keys) == first;
      });
    };
    auto const front = [](auto const& keys) -> auto const& {
      return keys.front();
    };
    auto const back = [](auto const& keys) -> auto const& {
      return keys.back();
    };
    while (common(front)
           and (keeps_all or is_infallible(keeping.front()->filter.front()))) {
      prefix.push_back(std::move(keeping.front()->filter.front()));
      for (auto* branch : keeping) {
        branch->filter.erase(branch->filter.begin());
        branch->keys.erase(branch->keys.begin());
      }
    }
    while (common(back)) {
      suffix.insert(suffix.begin(), std::move(keeping.front()->filter.back()));
      for (auto* branch : keeping) {
        branch->filter.pop_back();
        branch->keys.pop_back();
      }
    }
  }
  // A branch in front of the last one with the same filter need not be
  // distinguished from it.
  while (branches.size() > 1
         and branches[branches.size() - 2].same_filter(branches.back())) {
    branches.erase(branches.end() - 2);
  }
  // Route the events from the last branch to the first. Evaluating a branch
  // filter only if its condition is `true` and the next condition only if it is
  // not mirrors the routing. `filter if condition else false` is the chain
  // `condition, filter`, which the first branch keeps as such.
  auto routed_filter = OptimizeFilter{};
  if (branches.size() == 1) {
    routed_filter = std::move(branches.back().filter);
  } else {
    auto rest_source = filter_location(branches.back().filter);
    auto rest = conjoin_filter(std::move(branches.back().filter));
    for (auto& branch : std::views::reverse(branches) | std::views::drop(1)) {
      auto condition = std::move(*branch.condition);
      auto const source = filter_location(branch.filter);
      if (as_bool(rest) == false) {
        branch.filter.insert(branch.filter.begin(),
                             routing_predicate(std::move(condition)));
        if (&branch == &branches.front()) {
          routed_filter = std::move(branch.filter);
          break;
        }
        rest = conjoin_filter(std::move(branch.filter));
        relocate(rest, source);
        rest_source = source;
      } else if (branch.filter.empty()) {
        // `true if condition else rest` keeps the same events.
        rest = make_or(routing_predicate(std::move(condition)), std::move(rest),
                       rest_source);
      } else {
        rest_source = source.combine(rest_source);
        rest = make_if_else(conjoin_filter(std::move(branch.filter)),
                            std::move(condition), std::move(rest), source,
                            rest_source);
      }
    }
    if (routed_filter.empty()) {
      routed_filter.push_back(std::move(rest));
    }
  }
  auto result = std::move(prefix);
  result.insert(result.end(), std::move_iterator{routed_filter.begin()},
                std::move_iterator{routed_filter.end()});
  result.insert(result.end(), std::move_iterator{suffix.begin()},
                std::move_iterator{suffix.end()});
  result = simplify_filter(std::move(result));
  if (filter_size(result) > std::max(max_rewritten_predicate_size, largest)) {
    return None{};
  }
  return result;
}

} // namespace tenzir
