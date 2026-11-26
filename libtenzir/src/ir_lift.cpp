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
#include <caf/save_inspector_base.hpp>

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

/// Serializes like `caf::binary_serializer`, skipping `location` objects so
/// structurally identical expressions from different source positions share a
/// key.
class KeySerializer final : public caf::save_inspector_base<KeySerializer> {
public:
  explicit KeySerializer(caf::byte_buffer& buffer) : inner_{buffer} {
  }

  static constexpr auto has_human_readable_format() noexcept -> bool {
    return false;
  }

  auto set_error(caf::error stop_reason) -> void {
    inner_.set_error(std::move(stop_reason));
  }

  auto get_error() noexcept -> caf::error& {
    return inner_.get_error();
  }

  auto begin_object(caf::type_id_t type, std::string_view name) -> bool {
    if (skipped_ > 0 or name == "location") {
      ++skipped_;
      return true;
    }
    return inner_.begin_object(type, name);
  }

  auto end_object() -> bool {
    if (skipped_ > 0) {
      --skipped_;
      return true;
    }
    return inner_.end_object();
  }

  template <class... Ts>
  auto begin_field(Ts&&... xs) -> bool {
    return skipped_ > 0 or inner_.begin_field(std::forward<Ts>(xs)...);
  }

  auto end_field() -> bool {
    return skipped_ > 0 or inner_.end_field();
  }

  auto begin_tuple(size_t size) -> bool {
    return skipped_ > 0 or inner_.begin_tuple(size);
  }

  auto end_tuple() -> bool {
    return skipped_ > 0 or inner_.end_tuple();
  }

  auto begin_sequence(size_t size) -> bool {
    return skipped_ > 0 or inner_.begin_sequence(size);
  }

  auto end_sequence() -> bool {
    return skipped_ > 0 or inner_.end_sequence();
  }

  auto begin_associative_array(size_t size) -> bool {
    return skipped_ > 0 or inner_.begin_associative_array(size);
  }

  auto end_associative_array() -> bool {
    return skipped_ > 0 or inner_.end_associative_array();
  }

  auto begin_key_value_pair() -> bool {
    return skipped_ > 0 or inner_.begin_key_value_pair();
  }

  auto end_key_value_pair() -> bool {
    return skipped_ > 0 or inner_.end_key_value_pair();
  }

  template <class T>
  auto value(T&& x) -> bool {
    return skipped_ > 0 or inner_.value(std::forward<T>(x));
  }

private:
  caf::binary_serializer inner_;
  size_t skipped_ = 0;
};

/// Returns a key that two expressions share if and only if they are
/// structurally identical, ignoring source locations.
auto make_key(ast::expression const& expr) -> caf::byte_buffer {
  auto buffer = caf::byte_buffer{};
  auto f = KeySerializer{buffer};
  auto const success = f.apply(expr);
  TENZIR_ASSERT(success);
  return buffer;
}

auto keeps_none(ir::OptimizeFilter const& filter) -> bool {
  return filter.size() == 1 and as_bool(filter.front()) == false;
}

auto make_keys(ir::OptimizeFilter const& filter)
  -> std::vector<caf::byte_buffer> {
  auto keys = std::vector<caf::byte_buffer>{};
  keys.reserve(filter.size());
  for (auto const& predicate : filter) {
    keys.push_back(make_key(predicate));
  }
  return keys;
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
    return tenzir::keeps_none(filter);
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

auto ir::returns_events(pipeline const& branch, registry const& reg)
  -> Option<bool> {
  auto dh = null_diagnostic_handler{};
  auto output = branch.infer_type(events_element_type(), base_ctx{dh, reg});
  if (not output) {
    return None{};
  }
  return output->is<table_slice>() or output->is<nova::Events>();
}

auto ir::is_repeatable(ast::expression const& expr, registry const& reg)
  -> bool {
  auto checker = RepeatabilityChecker{reg};
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  checker.visit(const_cast<ast::expression&>(expr));
  return checker.repeatable;
}

auto ir::same_filter(OptimizeFilter const& lhs, OptimizeFilter const& rhs)
  -> bool {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (auto i = size_t{0}; i < lhs.size(); ++i) {
    if (make_key(lhs[i]) != make_key(rhs[i])) {
      return false;
    }
  }
  return true;
}

auto ir::lift_common_branch_filters(std::vector<OptimizeFilter>& filters,
                                    registry const& reg) -> OptimizeFilter {
  TENZIR_ASSERT(not filters.empty());
  auto keeping = std::vector<size_t>{};
  for (auto i = size_t{0}; i < filters.size(); ++i) {
    if (not keeps_none(filters[i])) {
      keeping.push_back(i);
    }
  }
  // If no branch keeps an event, `where false` drops all events before routing.
  if (keeping.empty()) {
    auto result = OptimizeFilter{};
    result.push_back(std::move(filters.front().front()));
    for (auto& filter : filters) {
      filter.clear();
    }
    return result;
  }
  // A predicate of a single branch that keeps events stays behind its
  // condition.
  if (keeping.size() < 2) {
    return {};
  }
  auto keys = std::vector<std::vector<caf::byte_buffer>>{};
  keys.reserve(keeping.size());
  for (auto index : keeping) {
    keys.push_back(make_keys(filters[index]));
  }
  // A predicate shared by every keeping branch can move before the routing
  // operator: always if it starts every branch (including no dead branches),
  // and from any position if evaluating it cannot fail.
  auto const keeps_all = keeping.size() == filters.size();
  auto prefix = OptimizeFilter{};
  auto positions = std::vector<size_t>(keeping.size(), 0);
  auto& first_filter = filters[keeping.front()];
  auto& first_keys = keys.front();
  for (auto idx = size_t{0}; idx < first_keys.size();) {
    auto const& key = first_keys[idx];
    auto in_all = true;
    auto at_front_of_all = keeps_all and idx == 0;
    positions[0] = idx;
    for (auto k = size_t{1}; k < keeping.size(); ++k) {
      auto it = std::ranges::find(keys[k], key);
      if (it == keys[k].end()) {
        in_all = false;
        break;
      }
      positions[k] = static_cast<size_t>(it - keys[k].begin());
      at_front_of_all = at_front_of_all and positions[k] == 0;
    }
    if (not in_all
        or (not at_front_of_all and not is_infallible(first_filter[idx]))) {
      ++idx;
      continue;
    }
    prefix.push_back(std::move(first_filter[idx]));
    for (auto k = size_t{0}; k < keeping.size(); ++k) {
      auto const pos = static_cast<ptrdiff_t>(positions[k]);
      filters[keeping[k]].erase(filters[keeping[k]].begin() + pos);
      keys[k].erase(keys[k].begin() + pos);
    }
  }
  return simplify_filter(std::move(prefix), reg);
}

auto ir::lift_branch_filters(std::vector<RoutedBranch> routed,
                             registry const& reg) -> Option<OptimizeFilter> {
  TENZIR_ASSERT(not routed.empty());
  auto largest = size_t{0};
  auto filters = std::vector<OptimizeFilter>{};
  filters.reserve(routed.size());
  for (auto& branch : routed) {
    auto const last = filters.size() + 1 == routed.size();
    TENZIR_ASSERT(last != branch.condition.has_value());
    largest = std::max(largest, filter_size(branch.filter));
    filters.push_back(std::move(branch.filter));
  }
  auto prefix = lift_common_branch_filters(filters, reg);
  auto branches = std::vector<Branch>{};
  branches.reserve(routed.size());
  for (auto i = size_t{0}; i < routed.size(); ++i) {
    auto keys = make_keys(filters[i]);
    branches.push_back(Branch{
      .condition = std::move(routed[i].condition),
      .filter = std::move(filters[i]),
      .keys = std::move(keys),
    });
  }
  // A predicate that ends every keeping branch applies to every event that the
  // rest of its branch filter keeps, even if it can fail, because it stays
  // behind the routed filter.
  auto suffix = OptimizeFilter{};
  {
    auto keeping = std::vector<Branch*>{};
    for (auto& branch : branches) {
      if (not branch.keeps_none()) {
        keeping.push_back(&branch);
      }
    }
    auto const common_back = [&] {
      if (keeping.size() < 2 or keeping.front()->keys.empty()) {
        return false;
      }
      auto const& back = keeping.front()->keys.back();
      return std::ranges::all_of(keeping, [&](Branch const* branch) {
        return not branch->keys.empty() and branch->keys.back() == back;
      });
    };
    while (common_back()) {
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
  // Adjacent branches with the same filter can share one entry: the second
  // condition runs only if the first does not route the event, just as `or`
  // short-circuits when its left operand is `true`.
  for (auto i = size_t{0}; i + 2 < branches.size();) {
    if (branches[i].same_filter(branches[i + 1])) {
      auto const source = branches[i].condition->get_location().combine(
        branches[i + 1].condition->get_location());
      branches[i].condition
        = make_or(routing_predicate(std::move(*branches[i].condition)),
                  routing_predicate(std::move(*branches[i + 1].condition)),
                  source);
      branches.erase(branches.begin() + static_cast<ptrdiff_t>(i + 1));
    } else {
      ++i;
    }
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
  result = simplify_filter(std::move(result), reg);
  if (filter_size(result) > std::max(max_rewritten_predicate_size, largest)) {
    return None{};
  }
  return result;
}

} // namespace tenzir
