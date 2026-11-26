//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

#include <vector>

/// Moves the residual filters of the branches of a routing operator, such as
/// `if` and `match`, in front of the operator.
namespace tenzir::ir {

/// Returns whether a branch of an operator like `if` or `match` returns events,
/// or `None` if it does not type-check for the events that the pipeline runs
/// with.
auto returns_events(pipeline const& branch, registry const& reg)
  -> Option<bool>;

/// Returns whether evaluating `expr` again for the same event yields the same
/// value, which requires that it is deterministic and moves no field.
auto is_repeatable(ast::expression const& expr, registry const& reg) -> bool;

/// A branch of an operator that routes each event to the first branch whose
/// condition is `true`, such as `if` and `match`.
struct RoutedBranch {
  /// The condition that routes an event to this branch unless an earlier
  /// branch takes it, or `None` for the last branch, which takes every
  /// remaining event. Values other than `true` do not route an event.
  Option<ast::expression> condition;

  /// The filter chain that the branch applies to the events that it takes.
  OptimizeFilter filter;
};

/// Returns whether two filter chains consist of structurally identical
/// predicates in the same order, ignoring source locations.
auto same_filter(OptimizeFilter const& lhs, OptimizeFilter const& rhs) -> bool;

/// Extracts predicates shared by every branch into a filter chain over the
/// input of the routing operator and removes them from `filters`.
///
/// Because the returned predicates apply regardless of which branch an event
/// takes, they do not depend on the routing conditions or scrutinee. If every
/// branch keeps no event, clears `filters` and returns `[false]`.
auto lift_common_branch_filters(std::vector<OptimizeFilter>& filters,
                                registry const& reg) -> OptimizeFilter;

/// Combines the filters of routed branches into a filter chain over the input
/// of the routing operator, which keeps exactly the events that the branch
/// filters keep.
///
/// The result evaluates the filter of a branch only for events that the branch
/// takes, and a condition only for events that the routing operator evaluates
/// it for. It may skip a condition for an event that it drops, as described
/// for `OptimizeRequest::filter`. Returns `None` if the result would exceed
/// both `max_rewritten_predicate_size` and the largest branch filter.
///
/// The conditions must be repeatable, as in `is_repeatable`, because the
/// routing operator evaluates them again.
auto lift_branch_filters(std::vector<RoutedBranch> branches,
                         registry const& reg) -> Option<OptimizeFilter>;

} // namespace tenzir::ir
