//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/box.hpp"
#include "tenzir/element_type.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/ast.hpp"

#include <vector>

namespace tenzir {

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
  /// The operator is responsible to report any type mismatches. If the
  /// operator could potentially accept the given input type, but the output
  /// type is not known yet, then `std::nullopt` may be returned.
  virtual auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>>;

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
  virtual auto spawn(element_type_tag input) && -> AnyOperator = 0;

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

  pipeline(std::vector<let> lets, std::vector<Box<Operator>> operators)
    : lets{std::move(lets)}, operators{std::move(operators)} {
  }

  /// @see Operator
  auto substitute(substitute_ctx ctx, bool instantiate) -> failure_or<void>;

  /// @see Operator
  auto spawn(element_type_tag input) && -> std::vector<AnyOperator>;

  /// @see Operator
  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>>;

  /// @see Operator
  auto
  optimize(optimize_filter filter, event_order order) && -> optimize_result;
};

struct optimize_result {
  /// The filter to be propageted to the upstream operator.
  optimize_filter filter;
  /// What ordering guarantees the operator needs from its upstream operator.
  event_order order;
  /// What the operator shall be replaced with.
  pipeline replacement;
};

} // namespace ir

template <>
inline constexpr auto enable_default_formatter<ir::pipeline> = true;

/// Plugin for transforming the AST of an operator invocation to its IR.
class operator_compiler_plugin : public virtual plugin {
public:
  /// Return the IR operator for the given AST invocation.
  ///
  /// Note that any `let` bindings in the arguments are not bound yet. This
  /// means that the implementation must call `expr.bind(ctx)` itself. The
  /// reason for that is that pipeline expressions can not be bound because the
  /// operator itself can introduce new bindings. Thus, we cannot bind inside
  /// pipeline expressions. For consistency, we decided to not bind anything.
  virtual auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>>
    = 0;

  /// Return the name of the operator, including `::` for modules.
  ///
  /// By default, this returns the name of the plugin.
  virtual auto operator_name() const -> std::string;
};

} // namespace tenzir
