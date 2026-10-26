//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/base_ctx.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::nova {

/// How a `let` binding that may look up secrets can be evaluated.
struct StartupBinding {
  /// Whether evaluation needs an operator context to look up a secret.
  bool requires_startup = false;
  /// Whether the binding is deterministic, treating secret lookups as stable
  /// within the invocation that resolves them and ignoring folded subtrees.
  bool deterministic = true;
};

/// Analyzes a binding and, if it requires startup, constant-folds every subtree
/// that does not, so that only secret lookups and the operations applied to
/// them remain.
auto fold_startup_binding(ast::expression& expr, base_ctx ctx)
  -> failure_or<StartupBinding>;

/// Evaluates `expr` without input. The result has exactly one row. Fails if
/// the expression references the input (`this`, fields, metadata) or if any
/// error diagnostic was emitted during evaluation.
auto const_eval_array(const ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<Array<Data>>;

/// Like `const_eval_array`, but materializes the single row as Nova `Data`
/// (an explicit `Null` becomes `Null`).
auto const_eval(const ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<Data>;

/// Evaluates a private copy with secrets resolved through the operator.
auto const_eval(const ast::expression& expr, OpCtx& ctx)
  -> Task<failure_or<Data>>;

/// Tries to evaluate a deterministic expression to a constant value. Emits
/// diagnostics only if the evaluation succeeded.
auto try_const_eval(const ast::expression& expr, InstantiateCtx ctx)
  -> Option<Data>;
} // namespace tenzir::nova
