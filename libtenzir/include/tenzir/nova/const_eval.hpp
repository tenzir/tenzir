//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

namespace tenzir::nova {

/// Evaluates `expr` without input. The result has exactly one row. Fails if
/// the expression references the input (`this`, fields, metadata) or if any
/// error diagnostic was emitted during evaluation.
auto const_eval_array(const ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<Array<Data>>;

/// Like `const_eval_array`, but materializes the single row as Nova `Data`
/// (an explicit `Null` becomes `Null`).
auto const_eval(const ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<Data>;

/// Tries to evaluate a deterministic expression to a constant value. Emits
/// diagnostics only if the evaluation succeeded.
auto try_const_eval(const ast::expression& expr, InstantiateCtx ctx)
  -> Option<Data>;
} // namespace tenzir::nova
