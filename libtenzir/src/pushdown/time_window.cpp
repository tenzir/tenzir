//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/time_window.hpp"

#include "tenzir/ir.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>

namespace tenzir::pushdown {

auto intersect(TimeWindow lhs, TimeWindow const& rhs) -> TimeWindow {
  if (rhs.begin and (not lhs.begin or *lhs.begin < *rhs.begin)) {
    lhs.begin = rhs.begin;
  }
  if (rhs.end and (not lhs.end or *lhs.end > *rhs.end)) {
    lhs.end = rhs.end;
  }
  return lhs;
}

auto time_window(ast::expression const& predicate, std::string_view field)
  -> TimeWindow {
  auto const* x = try_as<ast::binary_expr>(predicate);
  if (not x) {
    return {};
  }
  using enum ast::binary_op;
  if (x->op == and_) {
    return intersect(time_window(x->left, field), time_window(x->right, field));
  }
  if (x->op == or_) {
    auto const lhs = time_window(x->left, field);
    auto const rhs = time_window(x->right, field);
    auto result = TimeWindow{};
    if (lhs.begin and rhs.begin) {
      result.begin = std::min(*lhs.begin, *rhs.begin);
    }
    if (lhs.end and rhs.end) {
      result.end = std::max(*lhs.end, *rhs.end);
    }
    return result;
  }
  // The comparison as `field <op> constant`.
  auto op = x->op;
  auto const* constant = try_as<ast::constant>(x->right);
  auto const* other = &x->left;
  if (not constant) {
    constant = try_as<ast::constant>(x->left);
    other = &x->right;
    op = op == lt ? gt : op == leq ? geq : op == gt ? lt : op == geq ? leq : op;
  }
  auto const* instant = constant ? try_as<time>(constant->value) : nullptr;
  auto const path = ir::field_segments(*other);
  if (not instant or not path
      or fmt::to_string(fmt::join(*path, ".")) != field) {
    return {};
  }
  // A strict comparison beyond either extreme of a time admits no time.
  auto const none = TimeWindow{.begin = time::max(), .end = time::min()};
  switch (op) {
    case eq:
      return {.begin = *instant, .end = *instant};
    case gt:
      return *instant == time::max()
               ? none
               : TimeWindow{.begin = *instant + duration{1}, .end = None{}};
    case geq:
      return {.begin = *instant, .end = None{}};
    case lt:
      return *instant == time::min()
               ? none
               : TimeWindow{.begin = None{}, .end = *instant - duration{1}};
    case leq:
      return {.begin = None{}, .end = *instant};
    default:
      return {};
  }
}

auto in_units(TimeWindow const& window, duration unit, Round round)
  -> std::pair<Option<int64_t>, Option<int64_t>> {
  auto const u = unit.count();
  auto const floor = [&](time x) {
    auto const ns = x.time_since_epoch().count();
    return ns / u - (ns % u < 0 ? 1 : 0);
  };
  return {
    window.begin.transform([&](time x) {
      auto const whole = x.time_since_epoch().count() % u == 0;
      return floor(x) + (round == Round::inward and not whole ? 1 : 0);
    }),
    window.end.transform(floor),
  };
}

} // namespace tenzir::pushdown
