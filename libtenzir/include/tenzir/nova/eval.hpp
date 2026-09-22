//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/location.hpp"
#include "tenzir/nova/eval_ctx.hpp"
#include "tenzir/nova/instantiate_ctx.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/tql2/ast.hpp"

#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace tenzir::nova {

struct Events;
class Evaluator;
class LambdaArgument;
class AggregationInstance;

namespace _ {

class CallSite;
class EvalRun;

template <class Args, class Impl>
class AggregationInstanceImpl;

} // namespace _

/// The evaluated value of a function argument, produced for the rows of the
/// frame the function was called with, plus the location of the expression it
/// came from. Copying is cheap: the array shares its storage.
struct ValueArgument {
  Array<Data> data;
  ::tenzir::location source;

  /// An empty value. `Array<Data>` has no default constructor of its own
  /// (its storage variant has none), so this supplies the one every `Args`
  /// struct needs to be default-constructible. Argument values are filled
  /// before every call, so a function never observes this state.
  ValueArgument();

  ValueArgument(Array<Data> data, ::tenzir::location source);
};

/// A borrowed argument expression that the function evaluates itself, through
/// `EvalFrame::eval`, instead of receiving it as a `ValueArgument`. For
/// functions that decide which rows an operand is needed for, or whether it is
/// needed at all. Like `LambdaArgument`, the node belongs to the expression the
/// enclosing `Evaluator` owns.
class LazyArgument {
public:
  LazyArgument() = default;

  explicit LazyArgument(ast::expression const& node)
    : node_{std::addressof(node)} {
  }

  auto node() const -> ast::expression const& {
    TENZIR_ASSERT(node_);
    return *node_;
  }

  auto location() const -> ::tenzir::location {
    TENZIR_ASSERT(node_);
    return node_->get_location();
  }

  /// Whether an argument was provided; a default constructed `LazyArgument`
  /// stands for an omitted optional argument.
  explicit operator bool() const {
    return node_ != nullptr;
  }

private:
  ast::expression const* node_ = nullptr;
};

/// One node's place in an `_::EvalRun` (see `eval_internal.hpp`): the
/// surrounding `EvalCtx`, the input,
/// the rows to produce, and the ability to evaluate subexpressions over them.
///
/// The mask is part of the frame rather than a separate parameter, which
/// makes the central invariant structural: a frame's mask is always a subset
/// of the input's mask, because the only way to obtain a different one is
/// `narrow`.
///
/// Evaluating an expression under a frame yields an `Array<Data>` that is
/// valid exactly at the rows of `mask()`: every requested row holds a
/// materialized value, which may be an explicit `Null`. Rows outside the mask
/// hold unspecified values and must never be read.
class EvalFrame {
public:
  auto dh() const -> diagnostic_handler&;
  explicit(false) operator diagnostic_handler&() const;
  explicit(false) operator EvalCtx() const;
  auto ctx() const -> EvalCtx;

  /// The input events, or `nullptr` when evaluating without input.
  auto input() const -> Events const*;

  /// The rows to produce.
  auto mask() const& -> storage::BitMap const&;
  auto mask() && -> storage::BitMap;

  /// The physical row count all masks and arrays of this run share.
  auto length() const -> storage::Index;

  /// A frame restricted to `mask`, which must be a subset of `mask()`. This
  /// is how short-circuiting works: evaluate an operand under a frame
  /// narrowed to the rows that still need it.
  [[nodiscard]] auto narrow(storage::BitMap mask) const -> EvalFrame;

  /// Evaluates `expr` for `mask()`.
  auto eval(ast::expression const& expr) const -> Array<Data>;

  /// Evaluates a borrowed argument expression for `mask()`.
  auto eval(LazyArgument const& expr) const -> ValueArgument {
    return {eval(expr.node()), expr.location()};
  }

  /// Evaluates `lambda`'s body with its parameter bound to `subject`, for the
  /// rows of `subject.present`. The subject and `input` share a row layout;
  /// flattening is the caller's business, or use `eval_elements`.
  auto eval(LambdaArgument const& lambda, MaskedArray<Array<Data>> subject,
            Events const& input) const -> Array<Data>;

  /// Like above, but without input: the body may only read its parameter.
  auto eval(LambdaArgument const& lambda,
            MaskedArray<Array<Data>> subject) const -> Array<Data>;

  /// Evaluates `lambda` for every element of the rows of `list` selected by
  /// `mask()`, with the parameter bound to the element and captures broadcast
  /// from the element's row. Returns the flattened element results; pair them
  /// with `list.spans()` to rebuild a list column.
  auto eval_elements(LambdaArgument const& lambda,
                     storage::ListStorage const& list) const -> Array<Data>;

  /// An all-null result for `mask()`. Not `&&`-qualified: it only reads the
  /// frame's length, so a frame can hand out several.
  auto null() const -> Array<Data>;

  /// Calls `f` with a frame over `mask`, in a run over a synthesized input of
  /// `mask.length()` field-less rows that shares this run's evaluator and
  /// context. For evaluating over an array whose rows are not the input's,
  /// such as the elements of a list column. The frame is only valid inside
  /// `f`, and expressions evaluated under it see no fields.
  template <std::invocable<EvalFrame> F>
  auto detached(storage::BitMap mask, F f) const -> void {
    detached_impl(std::move(mask), std::addressof(f),
                  [](void* ctx, EvalFrame frame) {
                    std::invoke(*static_cast<F*>(ctx), std::move(frame));
                  });
  }

private:
  friend class _::EvalRun;
  friend class Evaluator;
  template <class Args, class Impl>
  friend class _::AggregationInstanceImpl;

  EvalFrame(_::EvalRun& run, storage::BitMap mask)
    : run_{std::addressof(run)}, mask_{std::move(mask)} {
  }

  auto detached_impl(storage::BitMap mask, void* ctx,
                     void (*f)(void*, EvalFrame)) const -> void;

  _::EvalRun* run_;
  storage::BitMap mask_;
};

/// A prepared expression and its mutable call sites. Prepared evaluators must
/// only be moved and evaluated sequentially.
class Evaluator {
public:
  Evaluator() = default;
  static auto make(ast::expression expression, InstantiateCtx ctx)
    -> failure_or<Evaluator>;

  Evaluator(Evaluator const&) = delete;
  Evaluator(Evaluator&&) noexcept;
  auto operator=(Evaluator const&) -> Evaluator& = delete;
  auto operator=(Evaluator&&) noexcept -> Evaluator&;
  ~Evaluator();

  /// Evaluates the prepared expression for the active rows in `events`.
  auto eval(Events const& events, EvalCtx ctx) -> Array<Data>;

  /// Evaluates the prepared expression without input events.
  auto eval(EvalCtx ctx) -> Array<Data>;

  /// Evaluates the prepared expression with input events when present.
  auto eval(Option<Events const&> events, EvalCtx ctx) -> Array<Data>;

  auto location() const -> ::tenzir::location {
    TENZIR_ASSERT(expression_);
    return expression_->get_location();
  }

private:
  friend class _::EvalRun;
  friend class AggregationInstance;
  template <class Args, class Impl>
  friend class _::AggregationInstanceImpl;

  explicit Evaluator(ast::expression expression);
  auto prepare(ast::expression& expression, InstantiateCtx ctx)
    -> failure_or<void>;
  auto call_site(ast::function_call const& call) -> _::CallSite&;

  Option<ast::expression> expression_;
  std::unordered_map<ast::function_call const*, Box<_::CallSite>> call_sites_;
};

inline auto EvalFrame::mask() const& -> storage::BitMap const& {
  return mask_;
}

inline auto EvalFrame::mask() && -> storage::BitMap {
  return std::move(mask_);
}

inline auto EvalFrame::length() const -> storage::Index {
  return mask_.length();
}

inline auto EvalFrame::narrow(storage::BitMap mask) const -> EvalFrame {
  TENZIR_ASSERT_EXPENSIVE(not mask.and_not(mask_).any());
  return EvalFrame{*run_, std::move(mask)};
}

inline auto EvalFrame::null() const -> Array<Data> {
  return Array<Data>{Array<Null>{storage::NullStorage{length()}}};
}

} // namespace tenzir::nova
