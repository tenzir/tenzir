//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/detail/saturating_arithmetic.hpp"
#include "tenzir/tql2/function_properties.hpp"

#include <algorithm>
#include <cstdint>

namespace tenzir::ir {

/// Expression properties computed in a single bottom-up pass by `ir::analyze`.
///
/// Forms a commutative monoid under `operator|` with default-constructed
/// `ExprProps{}` as the identity element (lattice join on qualitative fields,
/// saturating sum on `expensive_ops` and `node_count`). Every expression
/// returned by `ir::analyze` has `node_count >= 1`.
struct ExprProps {
  /// Highest volatility of any function call in the expression.
  Volatility volatility = Volatility::immutable;
  /// Highest per-row TQL fallibility over event input (assuming valid call
  /// instantiation).
  Fallibility fallibility = Fallibility::total;
  /// Highest evaluation cost tier of any node in the expression.
  Cost cost = Cost::plain;
  /// Number of expensive operations (expensive/unresolved calls, lambdas,
  /// pipeline expressions, format strings), including nested ones (saturating).
  /// Invariant for analyzed expressions: `expensive_ops > 0` iff
  /// `cost == Cost::expensive`.
  uint32_t expensive_ops = 0;
  /// Number of `ast::expression` nodes in the expression (saturating).
  uint32_t node_count = 0;
  /// Whether the expression contains a `move` unary operator.
  bool has_move = false;
  /// Whether the expression contains an unbound pipeline `$` variable
  /// (`ast::dollar_var`).
  bool has_unbound_var = false;
  /// Whether the expression contains a bare `ast::assignment` outside a
  /// `function_call`'s named argument list.
  bool has_bare_assignment = false;
  /// Whether the expression contains a non-value construct rejected even by
  /// legacy determinism (`pipeline_expr` or unresolved `function_call`).
  bool has_non_value = false;
  /// Whether the expression contains any `ast::function_call` (resolved or
  /// unresolved).
  bool has_function_call = false;
  /// Whether the expression references a secret (`std::secret(...)`,
  /// `ast::resolved_secret`, or a legacy `tenzir::secret` constant).
  bool has_secret = false;
  /// Whether the expression reads event data (`this`, `@meta`, or an
  /// unshadowed `root_field` in value position).
  bool reads_event = false;
  /// Whether every constant is accepted by the evaluator (`is_evaluable`) and
  /// no invalid value syntax node appears outside call-argument position.
  /// Multi-spread record literals keep `evaluable = true` (supported by the
  /// columnar evaluator) and set `fallibility = Fallibility::fails` for legacy
  /// conservatism.
  bool evaluable = true;
  /// Whether the expression is syntactically independent of the event and
  /// deterministic, matching `join`'s constant-key check (computed directly in
  /// the traversal so `ast::assignment` overrides it from its RHS alone).
  bool pure_constant = true;

  /// Whether re-evaluating the expression for the same event is guaranteed to
  /// yield the same value without field-removing (`move`) or secret-censoring
  /// effects.
  auto is_duplicable() const -> bool {
    return volatility == Volatility::immutable and not has_move
           and not has_unbound_var and not has_bare_assignment
           and not has_non_value and not has_secret;
  }

  /// Whether per-row evaluation over event input cannot fail the TQL pipeline,
  /// assuming every function call in the expression instantiates cleanly.
  ///
  /// Only sound when `not has_function_call` or after separately validating
  /// call instantiation (e.g., via `nova::Evaluator::make`). Do not use alone
  /// to erase an unvalidated call.
  auto is_eval_infallible() const -> bool {
    return evaluable and fallibility != Fallibility::fails and not has_non_value
           and not has_bare_assignment;
  }

  /// Whether evaluating the expression over event input cannot fail the TQL
  /// pipeline without validating function-call instantiation. Safe for both
  /// reordering and erasing unvalidated expressions.
  auto is_tql_infallible() const -> bool {
    return is_eval_infallible() and not has_function_call;
  }

  /// Whether the expression depends on no event data or runtime state and is
  /// eligible for optimizer constant folding (subject to diagnostic-free
  /// evaluation).
  auto is_const_foldable() const -> bool {
    return is_duplicable() and not reads_event and evaluable;
  }

  /// Whether the expression is syntactically independent of the event and
  /// deterministic (for join key and `on=` timestamp validation). Unlike
  /// `is_const_foldable()`, this includes runtime-evaluable constant forms that
  /// the optimizer does not fold or duplicate (such as `move 1`, multi-spread
  /// records, uncached package references, bare forms, and assignment RHS).
  auto is_pure_constant() const -> bool {
    return pure_constant;
  }

  /// Whether evaluating the expression costs about as much as reading a field
  /// or constant literal (free to duplicate unboundedly).
  auto is_plain() const -> bool {
    return cost == Cost::plain;
  }

  /// Whether the expression contains no expensive operations (acceptable to
  /// duplicate once).
  auto is_cheap() const -> bool {
    return cost <= Cost::cheap;
  }

  friend auto operator==(ExprProps const&, ExprProps const&) -> bool = default;

  friend auto operator|(ExprProps const& lhs, ExprProps const& rhs)
    -> ExprProps {
    return {
      .volatility = std::max(lhs.volatility, rhs.volatility),
      .fallibility = std::max(lhs.fallibility, rhs.fallibility),
      .cost = std::max(lhs.cost, rhs.cost),
      .expensive_ops
      = ::tenzir::detail::saturating_add(lhs.expensive_ops, rhs.expensive_ops),
      .node_count
      = ::tenzir::detail::saturating_add(lhs.node_count, rhs.node_count),
      .has_move = lhs.has_move or rhs.has_move,
      .has_unbound_var = lhs.has_unbound_var or rhs.has_unbound_var,
      .has_bare_assignment = lhs.has_bare_assignment or rhs.has_bare_assignment,
      .has_non_value = lhs.has_non_value or rhs.has_non_value,
      .has_function_call = lhs.has_function_call or rhs.has_function_call,
      .has_secret = lhs.has_secret or rhs.has_secret,
      .reads_event = lhs.reads_event or rhs.reads_event,
      .evaluable = lhs.evaluable and rhs.evaluable,
      .pure_constant = lhs.pure_constant and rhs.pure_constant,
    };
  }

  auto operator|=(ExprProps const& other) -> ExprProps& {
    *this = *this | other;
    return *this;
  }
};

/// Analyzes `expr` in a single bottom-up AST pass using function metadata from
/// `reg`. Never instantiates function plugins or performs I/O.
auto analyze(ast::expression const& expr, registry const& reg) -> ExprProps;

} // namespace tenzir::ir

namespace tenzir::detail {

/// Syntactic-only fallback when no `registry` is available. Used strictly by
/// the legacy `split_filter` and `split_filter_by_rewrite` helpers in `ir.cpp`
/// until Step 3 (TNZ-2247) replaces them with `ir::partition_filter`.
/// Treats every function call conservatively as `volatile_`, `fails`, and
/// `expensive`.
auto analyze_expr_without_registry(ast::expression const& expr)
  -> ir::ExprProps;

} // namespace tenzir::detail
