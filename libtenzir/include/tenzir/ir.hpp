//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/fwd.hpp"
#include "tenzir/element_type.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

#include <concepts>
#include <span>
#include <type_traits>
#include <vector>

namespace tenzir {

// Forward declaration to avoid including pipeline.hpp.
enum class event_order;

namespace ir {

/// A chain of predicates used during the optimization process.
///
/// The sequence shall be interpreted as a sequence of `where <expr>` operators,
/// which implies that subsequent expressions are not evaluated if a previous
/// one already filtered an event out.
using optimize_filter = std::vector<ast::expression>;

/// Base class for all IR operators.
class Operator {
public:
  virtual ~Operator() = default;

  /// Return the name of a matching serialization plugin.
  virtual auto name() const -> std::string = 0;

  /// A virtual copy constructor.
  virtual auto copy() const -> Box<Operator>;

  /// A virtual move constructor.
  virtual auto move() && -> Box<Operator>;

  /// Return the output type of this operator for a given input type.
  ///
  /// The operator is responsible to report any type mismatches. This is only
  /// called after instantiation, so the output type is always determinable.
  virtual auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag>
    = 0;

  /// Substitute variables from the context and potentially instantiate `this`.
  ///
  /// If `instantiate == true`, then the operator shall be instantiated. That
  /// indicates that non-deterministic arguments, such as `now()`, shall be
  /// evaluated. Whether it also leads to instantiation of subpipelines
  /// depends on the operator. For example, the implementation of `if` also
  /// instantiates its subpipelines, but `every` does not.
  virtual auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void>
    = 0;

  /// Return a potentially optimized version of this operator.
  ///
  /// TODO: Describe this in more detail.
  virtual auto
  optimize(optimize_filter filter, event_order order) && -> optimize_result;

  /// Return the executable matching this operator.
  ///
  /// The implementation may assume that the operator was previously
  /// instantiated, i.e., `substitute` was called with `instantiate == true`.
  /// However, other methods such as `optimize` may be called in between.
  virtual auto spawn(element_type_tag input) const -> AnyOperator = 0;

  /// Return the "main location" of the operator.
  ///
  /// Typically, this is the operator name. If there is no operator name, for
  /// example in the case of a simple assignment, return the location that
  /// should be used in diagnostics.
  ///
  /// TODO: Should we store this externally?
  /// TODO: Make it pure virtual.
  virtual auto main_location() const -> location {
    return location::unknown;
  }
};

/// The IR representation of a `let` statement.
struct let {
  let() = default;

  let(ast::identifier ident, ast::expression expr, let_id id)
    : ident{std::move(ident)}, expr{std::move(expr)}, id{id} {
  }

  friend auto inspect(auto& f, let& x) -> bool {
    return f.object(x).fields(f.field("ident", x.ident),
                              f.field("expr", x.expr), f.field("id", x.id));
  }

  ast::identifier ident;
  ast::expression expr;
  let_id id;
};

/// The IR representation of a pipeline.
struct pipeline {
  std::vector<let> lets;
  std::vector<Box<Operator>> operators;

  friend auto inspect(auto& f, pipeline& x) -> bool {
    return f.object(x).fields(f.field("lets", x.lets),
                              f.field("operators", x.operators));
  }

  pipeline() = default;

  pipeline(std::vector<let> lets, std::vector<Box<Operator>> operators);

  /// @see Operator
  auto substitute(substitute_ctx ctx, bool instantiate) -> failure_or<void>;

  /// @see Operator
  auto spawn(element_type_tag input) && -> std::vector<AnyOperator>;

  /// @see Operator
  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag>;

  // TODO: How do we take care that we don't propagate $-vars past the point
  // where they will be defined?
  /// @see Operator
  auto
  optimize(optimize_filter filter, event_order order) && -> optimize_result;
};

struct CompileResult {
  CompileResult() = default;

  CompileResult(Box<Operator> op);

  template <class Op>
    requires(std::derived_from<std::remove_cvref_t<Op>, Operator>
             and not std::same_as<std::remove_cvref_t<Op>, Operator>)
  CompileResult(Op&& op) {
    pipeline_.operators.push_back(std::forward<Op>(op));
  }

  CompileResult(pipeline pipe);

  auto unwrap() && -> pipeline;

private:
  ir::pipeline pipeline_;
};

struct optimize_result {
  /// The filter to be propageted to the upstream operator.
  optimize_filter filter;
  /// What ordering guarantees the operator needs from its upstream operator.
  event_order order;
  /// What the operator shall be replaced with.
  pipeline replacement;
};

struct split_filter_result {
  ir::optimize_filter independent;
  ir::optimize_filter dependent;
};

/// Splits a filter chain into independent and dependent parts.
/// A filter expression is dependent if its references overlap with `touched`.
/// If refs of a filter cannot be determined (ambiguous), it is conservatively
/// placed into the dependent set.
auto split_filter_by_dependents(ir::optimize_filter filter,
                                const ast::ExprRefs& touched)
  -> split_filter_result;

class SetIr final : public Operator {
public:
  SetIr();

  explicit SetIr(std::vector<ast::assignment> assignments);

  auto name() const -> std::string override;

  auto copy() const -> Box<Operator> override;

  auto move() && -> Box<Operator> override;

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override;

  auto spawn(element_type_tag input) const -> AnyOperator override;

  auto optimize(optimize_filter filter,
                event_order order) && -> optimize_result override;

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override;

  template <class Inspector>
  friend auto inspect(Inspector& f, SetIr& x) -> bool;

private:
  std::vector<ast::assignment> assignments_;
  event_order order_;
};

} // namespace ir

/// Create a `set` IR operator from assignments.
auto make_set_ir(std::vector<ast::assignment> assignments) -> Box<ir::Operator>;

/// Create a `where` operator with the given expression.
auto make_where_ir(ast::expression filter) -> Box<ir::Operator>;

template <>
inline constexpr auto enable_default_formatter<ir::pipeline> = true;

} // namespace tenzir
