//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"
#include "tenzir/time.hpp"
#include "tenzir/tql2/ast.hpp"

#include <cstdint>
#include <string_view>
#include <utility>

/// The time windows that filters select, for sources whose requests take
/// time bounds.
namespace tenzir::pushdown {

/// The bounds of the times that a filter admits, both inclusive. A side
/// without a bound is open.
struct TimeWindow {
  Option<time> begin = None{};
  Option<time> end = None{};

  /// Whether the window admits no time.
  auto empty() const -> bool {
    return begin and end and *begin > *end;
  }
};

/// Returns the window that both `lhs` and `rhs` admit.
auto intersect(TimeWindow lhs, TimeWindow const& rhs) -> TimeWindow;

/// Returns the window of the times of `field`, a dotted field path, for which
/// `predicate` may hold, as far as its comparisons of the field with time
/// literals tell. `and` intersects the windows of its operands, and `or`
/// encloses them. A strict bound moves by a nanosecond, and a strict bound
/// beyond either extreme of a time admits no time.
auto time_window(ast::expression const& predicate, std::string_view field)
  -> TimeWindow;

/// How `in_units` rounds a window.
enum class Round {
  /// To the whole units within the window, for targets whose times are whole
  /// units.
  inward,
  /// To the units that overlap the window, for requests of a superset.
  outward,
};

/// Returns `window` as the inclusive range of the counts of `unit` since the
/// epoch, where an open side is `None`.
auto in_units(TimeWindow const& window, duration unit, Round round)
  -> std::pair<Option<int64_t>, Option<int64_t>>;

} // namespace tenzir::pushdown
