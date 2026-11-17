//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ip.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pushdown/layout.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/time.hpp"
#include "tenzir/tql2/ast.hpp"

#include <folly/Function.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>

/// Spelling of TQL expressions in the language of a target, with the
/// semantics of the target.
///
/// `ExpressionRenderer` walks the syntax of an expression and leaves every
/// spelling to an `ExpressionDialect`: literals, operators, null checks, lists,
/// and calls. A dialect vetoes what it cannot spell by returning `None`, which
/// makes the whole expression unspellable. Fields bind through a callback,
/// since what a field names depends on where the expression appears, such as
/// in a stage after a renaming one.
namespace tenzir::pushdown {

class ExpressionRenderer;

/// The spelling of a TQL function in a target, with holes for its arguments:
/// `{i}` is argument `i` as a value, and `{i:pred}` as a predicate. With
/// `{i:s}`, `{i:ms}`, `{i:us}`, or `{i:ns}`, it is a constant duration as a
/// whole, positive count of the unit, but no multiple of a week or a month,
/// which TQL buckets by the calendar. A hole may repeat, as in
/// `(LEFT({0}, LEN({1})) = {1})`. A call whose argument does not fit its hole
/// has no spelling.
struct Function {
  std::string_view tql;
  uint8_t arity = 0;
  std::string_view spelling;
  /// Whether the spelling is a predicate rather than a value.
  bool predicate = false;
  /// How a group key that the function computes decodes.
  Decode decode = Decode::native;
};

/// Returns the width of a time bucket that `expr` is: a constant, positive
/// duration, but no multiple of a week or a month, which TQL buckets by the
/// calendar.
auto bucket_width(ast::expression const& expr) -> Option<duration>;

/// The spelling of a TQL aggregation function, with holes as for `Function`.
struct Aggregation {
  std::string_view tql;
  uint8_t arity = 0;
  std::string_view spelling;
  Decode decode = Decode::native;
};

/// Returns the row of `rows` for `name` with `arity` arguments, if any.
template <class Row>
auto find_row(std::span<Row const> rows, std::string_view name, size_t arity)
  -> Row const* {
  auto it = std::ranges::find_if(rows, [&](Row const& row) {
    return row.tql == name and row.arity == arity;
  });
  return it == rows.end() ? nullptr : &*it;
}

/// Fills the holes of `spelling` for `args` with `fill`, which receives the
/// index of an argument and whether its hole is a predicate. Returns `None` if
/// `fill` does.
auto spell(std::string_view spelling, std::span<ast::expression const> args,
           folly::FunctionRef<auto(size_t, bool)->Option<std::string>> fill)
  -> Option<std::string>;

/// The spelling of expressions in the language of a target.
///
/// A binary expression becomes `(<left> <op> <right>)`, with the operator from
/// `binary_operator`, unless `render_binary` spells it. The other forms are up
/// to the hooks below.
class ExpressionDialect {
public:
  virtual ~ExpressionDialect() = default;

  // -- Literals ---------------------------------------------------------------

  /// Spells the literal `null`.
  virtual auto render_null() const -> Option<std::string> = 0;

  /// Spells a boolean literal.
  virtual auto render_bool(bool value) const -> std::string = 0;

  /// Spells an integer literal, or returns `None` if the target cannot.
  virtual auto render_int(int64_t value) const -> Option<std::string> = 0;

  /// Spells an unsigned integer literal, or returns `None` if the target
  /// cannot.
  virtual auto render_uint(uint64_t value) const -> Option<std::string> = 0;

  /// Whether the target reads `value`, a finite `double`, as `render_double`
  /// spells it. Defaults to `true`.
  virtual auto accepts_double(double value) const -> bool;

  /// Spells a finite `double`.
  virtual auto render_double(double value) const -> std::string = 0;

  /// Spells a string literal, or returns `None` if the dialect cannot spell
  /// `text`.
  virtual auto quote_string(std::string_view text) const -> Option<std::string>
    = 0;

  /// Spells a time, a duration, an IP address, or a subnet literal. Each
  /// defaults to vetoing.
  virtual auto render_time(time value) const -> Option<std::string>;
  virtual auto render_duration(duration value) const -> Option<std::string>;
  virtual auto render_ip(ip value) const -> Option<std::string>;
  virtual auto render_subnet(subnet value) const -> Option<std::string>;

  // -- Operators --------------------------------------------------------------

  /// Spells the binary expression `x` where its spelling depends on its
  /// operands, such as an operator that the target spells differently for a
  /// literal operand. Returns `None` to veto `x`, or an empty spelling to
  /// leave `x` to the default spelling. `renderer` spells the operands. The
  /// spelling of an arithmetic operator is a value, that of any other
  /// operator a predicate. Defaults to the default spelling.
  virtual auto render_binary(ast::binary_expr const& x,
                             ExpressionRenderer const& renderer) const
    -> Option<Option<std::string>>;

  /// Spells the operator of a binary expression, such as `=` for `==`, or
  /// returns `None` if the target cannot compute it, as for arithmetic that a
  /// dialect vetoes. Never receives `in`, `if`, or `else`, and receives `==`
  /// and `!=` only when neither side is `null`.
  virtual auto binary_operator(ast::binary_op op) const
    -> Option<std::string_view>
    = 0;

  /// Spells the negation of the predicate `operand`.
  virtual auto render_not(std::string_view operand) const -> Option<std::string>
    = 0;

  /// Spells the arithmetic negation of the value `operand`.
  virtual auto render_negate(std::string_view operand) const
    -> Option<std::string>
    = 0;

  /// Spells the predicate that `operand` is `null`, or with `negated`, that it
  /// is not.
  virtual auto render_null_check(std::string_view operand, bool negated) const
    -> Option<std::string>
    = 0;

  /// Spells the predicate that `operand` equals one of `elements`, which are
  /// rendered literals and not empty.
  virtual auto render_in(std::string_view operand,
                         std::span<std::string const> elements) const
    -> Option<std::string>
    = 0;

  // -- Calls and classification -----------------------------------------------

  /// The functions that the dialect spells. Defaults to none.
  virtual auto functions() const -> std::span<Function const>;

  /// The aggregation functions that the dialect spells. Only functions whose
  /// result does not depend on the order of their input qualify. Defaults to
  /// none.
  virtual auto aggregations() const -> std::span<Aggregation const>;

  /// Spells a call of a function of the standard library, with the receiver
  /// of a method call first. Defaults to the row of `functions()`.
  virtual auto render_call(ast::function_call const& call,
                           ExpressionRenderer const& renderer) const
    -> Option<std::string>;

  /// Whether the target computes with time literals. A dialect that spells
  /// them as strings that the target casts to the type of the other operand
  /// of a comparison returns `false`, which vetoes arithmetic with them.
  /// Defaults to `true`.
  virtual auto time_literal_arithmetic() const -> bool;

  /// Whether a boolean value may stand where the dialect expects a predicate,
  /// as in `WHERE flag`, and a predicate where it expects a value, as in
  /// `(x > 1) = TRUE`.
  virtual auto boolean_values() const -> bool = 0;
};

/// Spells expressions of TQL in an `ExpressionDialect`, with the semantics of
/// the target, as far as their syntax allows. Fields bind through `bind`,
/// which receives the segments of a field path without `?`.
class ExpressionRenderer {
public:
  using Binder
    = std::function<auto(std::span<std::string const>)->Option<std::string>>;

  ExpressionRenderer(ExpressionDialect const& dialect, Binder bind);

  /// Spells `expr` as a predicate.
  auto predicate(ast::expression const& expr) const -> Option<std::string>;

  /// Spells `expr` as a value.
  auto value(ast::expression const& expr) const -> Option<std::string>;

  /// Spells a literal.
  auto literal(ast::constant::kind const& value) const -> Option<std::string>;

  /// Spells `row` with `args`, filling each hole with `value` or `predicate`.
  template <class Row>
  auto spell(Row const& row, std::span<ast::expression const> args) const
    -> Option<std::string> {
    return pushdown::spell(row.spelling, args, [&](size_t i, bool predicate) {
      return predicate ? this->predicate(args[i]) : value(args[i]);
    });
  }

private:
  /// Rendered text, and whether it is a predicate rather than a value.
  struct Fragment {
    std::string text;
    bool predicate = false;
  };

  auto render_literal(ast::constant::kind const& value) const
    -> Option<Fragment>;
  auto field(ast::expression const& expr) const -> Option<Fragment>;
  auto list(ast::expression const& expr) const
    -> Option<std::vector<std::string>>;
  auto binary(ast::binary_expr const& x) const -> Option<Fragment>;
  auto call(ast::function_call const& x) const -> Option<Fragment>;
  auto render(ast::expression const& expr) const -> Option<Fragment>;

  Ref<ExpressionDialect const> dialect_;
  Binder bind_;
};

} // namespace tenzir::pushdown
