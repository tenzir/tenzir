//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/instantiate_ctx.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/variant.hpp"

#include <span>
#include <string_view>
#include <vector>

namespace tenzir::nova {

/// Marks a lambda body that reads the enclosing event as a whole, through
/// `this`. No set of field paths describes what such a body needs.
struct ThisTag {
  location source;
};

/// What a lambda body reads from the enclosing input: the field paths, none
/// of which is a prefix of another, or the whole event.
using LambdaCaptures = variant<std::span<ast::field_path const>, ThisTag>;

/// The owning counterpart of `LambdaCaptures`.
using LambdaCaptureStorage = variant<std::vector<ast::field_path>, ThisTag>;

namespace _ {

inline auto borrow(LambdaCaptureStorage const& captures) -> LambdaCaptures {
  return match(
    captures,
    [](std::vector<ast::field_path> const& paths) -> LambdaCaptures {
      return std::span<ast::field_path const>{paths};
    },
    [](ThisTag capture) -> LambdaCaptures {
      return capture;
    });
}

} // namespace _

/// A prepared unary lambda: the borrowed AST node plus its capture analysis.
///
/// A lambda is not an evaluator of its own. Its body belongs to the
/// expression that the enclosing `Evaluator` owns and prepared, so its call
/// sites live in that evaluator's function table and its evaluation is one
/// more `EvalRun` over the same expression. Consequently a `LambdaArgument`
/// must not outlive the expression it points into, and the node must stay in
/// place: unlike constant arguments, instantiation borrows it instead of moving
/// it out of the call.
///
/// Apply one with `EvalFrame::eval` (bind the parameter to a subject array)
/// or `EvalFrame::eval_elements` (bind it to each element of a list column).
class LambdaArgument {
public:
  LambdaArgument() = default;

  /// Records the parameter and free fields of `lambda`, which must be unary.
  /// Only a pointer to the node is stored; `lambda` is left in place and must
  /// outlive the returned handle. Takes a mutable reference because the
  /// capture analysis runs on `ast::visitor`, not because it modifies the
  /// node.
  static auto make(ast::lambda_expr& lambda, InstantiateCtx ctx)
    -> failure_or<LambdaArgument>;

  /// The name the subject is bound to.
  auto param() const -> std::string_view {
    TENZIR_ASSERT(node_);
    return node_->param(0).name;
  }

  auto body() const -> ast::expression const& {
    TENZIR_ASSERT(node_);
    return node_->body;
  }

  /// What the body reads from the enclosing input.
  auto captures() const -> LambdaCaptures {
    return _::borrow(captures_);
  }

  auto location() const -> ::tenzir::location {
    TENZIR_ASSERT(node_);
    return node_->location;
  }

  /// Whether an argument was provided; a default constructed `LambdaArgument`
  /// stands for an omitted optional argument.
  explicit operator bool() const {
    return node_ != nullptr;
  }

private:
  LambdaArgument(ast::lambda_expr const* node, LambdaCaptureStorage captures)
    : node_{node}, captures_{std::move(captures)} {
  }

  ast::lambda_expr const* node_ = nullptr;
  LambdaCaptureStorage captures_;
};

/// A prepared binary lambda, such as the `cmp` comparator of `sort`.
///
/// Shares the lifetime rules of `LambdaArgument`: the node is borrowed, not
/// owned. Both parameters are bound, so neither appears in `captures()`.
/// Apply one with the binary `EvalFrame::eval`.
class BinaryLambdaArgument {
public:
  BinaryLambdaArgument() = default;

  /// Records the parameters and free fields of `lambda`, which must be binary.
  static auto make(ast::lambda_expr& lambda, InstantiateCtx ctx)
    -> failure_or<BinaryLambdaArgument>;

  /// The name the left or right subject is bound to.
  auto param(size_t index) const -> std::string_view {
    TENZIR_ASSERT(node_);
    TENZIR_ASSERT(index < 2);
    return node_->param(index).name;
  }

  auto body() const -> ast::expression const& {
    TENZIR_ASSERT(node_);
    return node_->body;
  }

  /// What the body reads from the enclosing input.
  auto captures() const -> LambdaCaptures {
    return _::borrow(captures_);
  }

  auto location() const -> ::tenzir::location {
    TENZIR_ASSERT(node_);
    return node_->location;
  }

  /// Whether an argument was provided; a default constructed
  /// `BinaryLambdaArgument` stands for an omitted optional argument.
  explicit operator bool() const {
    return node_ != nullptr;
  }

private:
  BinaryLambdaArgument(ast::lambda_expr const* node,
                       LambdaCaptureStorage captures)
    : node_{node}, captures_{std::move(captures)} {
  }

  ast::lambda_expr const* node_ = nullptr;
  LambdaCaptureStorage captures_;
};

} // namespace tenzir::nova
