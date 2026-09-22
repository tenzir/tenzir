//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

/// The machinery behind `Evaluator` and `EvalFrame`: prepared call sites and
/// the per-batch run that frames point into. Function implementations only
/// need `eval.hpp`; this header is for the evaluator itself and for the code
/// that builds call sites.

#include "tenzir/any.hpp"
#include "tenzir/box.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/type_list.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/tql2/ast.hpp"

#include <functional>
#include <memory>
#include <utility>
#include <vector>

namespace tenzir::nova::_ {

/// One argument expression the framework evaluates before every call, and
/// where to store the result in the argument bundle.
struct ValueSlot {
  ast::expression const* expr = nullptr;
  std::function<auto(Any&, ValueArgument)->void> store;
};

/// A prepared call site: the argument bundle of one concrete call plus the
/// kernel that consumes it. Created by `FunctionDescription::instantiate`,
/// owned by the `Evaluator` that prepared the enclosing expression, and
/// reused across batches.
class CallSite {
public:
  /// Runs a function over a type-erased argument bundle. One kernel serves
  /// every call site of its function, so all call-specific state lives in the
  /// bundle.
  using Kernel = auto (*)(Any const& args, EvalFrame frame) -> Array<Data>;

  /// Builds the `AggregationInstance` of an aggregation call once the
  /// expression it is the root of has been prepared into `evaluator`. Like
  /// `Kernel`, one function per implementation type. Null for functions.
  using AggregationFactory
    = auto (*)(Evaluator evaluator, ast::function_call const& root)
      -> Box<AggregationInstance>;

  CallSite(Any args, std::vector<ValueSlot> slots, Kernel kernel)
    : args_{std::move(args)}, slots_{std::move(slots)}, kernel_{kernel} {
    TENZIR_ASSERT(kernel_);
  }

  /// Evaluates the argument expressions for `frame.mask()` and runs the
  /// function over the results.
  auto eval(EvalFrame frame) -> Array<Data>;

  /// Evaluates the argument expressions for `frame.mask()` into the bundle
  /// and returns it. The values are valid until the next call.
  auto fill(EvalFrame const& frame) -> Any const&;

  /// The prepared argument bundle. Only the constants are meaningful outside
  /// a call.
  template <class Args>
  auto args() const -> Args const& {
    return args_.as<Args>();
  }

  auto set_aggregation(AggregationFactory factory) -> void {
    TENZIR_ASSERT(factory);
    aggregation_ = factory;
  }

  /// The aggregation factory, or null if this is a function call site.
  auto aggregation() const -> AggregationFactory {
    return aggregation_;
  }

private:
  Any args_;
  std::vector<ValueSlot> slots_;
  Kernel kernel_;
  AggregationFactory aggregation_ = nullptr;
};

/// One traversal of a prepared expression over one batch. Instances borrow
/// their expression, call-site state, and input from an `Evaluator` and must
/// not outlive a call to `Evaluator::eval`. Each node of the traversal gets
/// its own `EvalFrame` into this run.
class EvalRun {
public:
  /// The services borrowed for this run.

  auto ctx() const -> EvalCtx {
    return ctx_;
  }

  auto input() const -> Events const* {
    return input_;
  }

  /// Evaluates `expr` under `frame`. Node implementations recurse through
  /// `frame.eval` (or `frame.narrow(...).eval`) instead of calling this
  /// directly.
  auto eval(ast::expression const& expr, EvalFrame frame) -> Array<Data>;

  explicit(false) operator diagnostic_handler&() {
    return ctx_;
  }

private:
  auto eval(const ast::field_access&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::index_expr&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::this_&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::root_field&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::function_call&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::unary_expr&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::binary_expr&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::constant&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::format_expr&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::record&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::list&, EvalFrame frame) -> Array<Data>;
  auto eval(const ast::meta&, EvalFrame frame) -> Array<Data>;

  template <class T>
    requires(::tenzir::detail::tl_contains<ast::expression_kinds, T>::value)
  auto eval(T const& x, EvalFrame frame) -> Array<Data> {
    diagnostic::warning("eval not implemented yet for: {:?}",
                        use_default_formatter<T>(x))
      .primary(x)
      .emit(ctx_);
    return frame.null();
  }

  friend class nova::Evaluator;
  friend class nova::EvalFrame;
  template <class Args, class Impl>
  friend class AggregationInstanceImpl;

  EvalRun(Evaluator& evaluator, Events const* input, EvalCtx ctx);

  auto fail_not_constant(location loc, EvalFrame frame) const -> Array<Data>;

  Evaluator* evaluator_;
  Events const* input_ = nullptr;
  EvalCtx ctx_;
};

} // namespace tenzir::nova::_
