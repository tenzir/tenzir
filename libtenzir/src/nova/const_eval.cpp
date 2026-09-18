//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/const_eval.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/try.hpp"

#include <utility>

#include "diagnostics.hpp"

namespace tenzir::nova {

auto const_eval_array(const ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<Array<Data>> {
  auto diagnostics = _::DiagnosticScope{ctx};
  TRY(auto evaluator,
      Evaluator::make(expr, InstantiateCtx{diagnostics.handler(), ctx}));
  auto result = evaluator.eval(EvalCtx{diagnostics.handler()});
  // Errors that did not abort the evaluation (e.g. a function that is not
  // implemented for nova) still make the value meaningless.
  if (diagnostics.failed()) {
    return failure::promise();
  }
  TENZIR_ASSERT(result.length() == 1);
  return result;
}

auto const_eval(const ast::expression& expr, InstantiateCtx ctx)
  -> failure_or<located<data>> {
  TRY(auto result, const_eval_array(expr, ctx));
  // The single row is always requested, so it always holds a value;
  // `materialize` renders an explicit `Null` as `data{}`.
  auto value = materialize(result.get(0));
  return located{std::move(value), expr.get_location()};
}

auto try_const_eval(const ast::expression& expr, InstantiateCtx ctx)
  -> Option<located<data>> {
  const registry& reg = ctx;
  if (not expr.is_deterministic(reg)) {
    return None{};
  }
  auto collector = collecting_diagnostic_handler{};
  auto result = const_eval(expr, InstantiateCtx{collector, reg});
  if (not result) {
    return None{};
  }
  std::move(collector).forward_to(ctx);
  return std::move(*result);
}

} // namespace tenzir::nova
