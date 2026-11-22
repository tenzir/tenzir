//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ip.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pushdown/layout.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/time.hpp"
#include "tenzir/tql2/ast.hpp"

#include <folly/Function.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

/// The spelling of a TQL function in a target, with holes for its arguments.
///
/// A hole names a positional argument by its index, as in `{0}`, or a named
/// argument by its name, as in `{end}`, and spells it as a value. A kind after
/// a colon changes that:
///
/// - `{0:pred}` spells the argument as a predicate.
/// - `{0:expr}` spells the argument as an expression value (e.g. a column
///   reference inside an inline `eval` expression of a piped aggregation).
/// - `{0:nat}` requires a constant integer that is not negative, and spells
///   it as a decimal number.
/// - `{0:s}`, `{0:us}`, and `{0:ns}` require a constant duration
///   that is a whole, positive count of the unit, but no multiple of a week or
///   a month, which TQL buckets by the calendar, and spell the count.
///
/// A hole may repeat, as in `(LEFT({0}, LEN({1})) = {1})`. A row applies to a
/// call with `arity` positional arguments whose named arguments are exactly
/// those that its holes name, so a function with optional named arguments has
/// a row for each combination that the target spells. A call whose argument
/// does not fit its hole has no spelling.
///
/// A spelling computes with the semantics of the target, which may differ
/// from TQL's: its types, how it treats `null`, its result for no input, and
/// its arithmetic, such as how it rounds, compares text, or maps case.
struct Function {
  std::string_view tql;
  uint8_t arity = 0;
  std::string_view spelling;
  /// How the value that the spelling computes decodes.
  Decode decode = Decode::native;
  /// Whether the spelling is a predicate rather than a value.
  bool predicate = false;
};

/// The spelling of a TQL aggregation function, with holes as for `Function`.
using Aggregation = Function;

/// Whether `spelling` is well-formed for a row with `arity` positional
/// arguments: every hole closes, names a positional argument below `arity` or
/// a named argument, and has a known kind. Rows check this at compile time, as
/// in `static_assert(valid_rows(rows))`.
constexpr auto valid_spelling(std::string_view spelling, size_t arity) -> bool {
  constexpr auto npos = std::string_view::npos;
  constexpr auto kinds = std::array<std::string_view, 7>{
    "", "pred", "expr", "nat", "s", "us", "ns",
  };
  auto const digit = [](char c) {
    return c >= '0' and c <= '9';
  };
  auto const word = [&](char c) {
    return digit(c) or c == '_' or (c >= 'a' and c <= 'z')
           or (c >= 'A' and c <= 'Z');
  };
  while (true) {
    auto const open = spelling.find('{');
    if (spelling.substr(0, open).find('}') != npos) {
      return false;
    }
    if (open == npos) {
      return true;
    }
    auto const close = spelling.find('}', open);
    if (close == npos) {
      return false;
    }
    auto const inner = spelling.substr(open + 1, close - open - 1);
    auto const colon = inner.find(':');
    auto const argument = inner.substr(0, colon);
    auto const kind
      = colon == npos ? std::string_view{} : inner.substr(colon + 1);
    if (argument.empty() or not std::ranges::all_of(argument, word)
        or std::ranges::find(kinds, kind) == kinds.end()) {
      return false;
    }
    if (std::ranges::all_of(argument, digit)) {
      auto index = size_t{0};
      for (auto c : argument) {
        index = index * 10 + static_cast<size_t>(c - '0');
      }
      if (index >= arity) {
        return false;
      }
    } else if (digit(argument.front())) {
      return false;
    }
    spelling.remove_prefix(close + 1);
  }
}

/// Whether every row of `rows` has a well-formed spelling.
consteval auto valid_rows(std::span<Function const> rows) -> bool {
  return std::ranges::all_of(rows, [](Function const& row) {
    return valid_spelling(row.spelling, row.arity);
  });
}

/// Returns the width of a time bucket that `expr` is: a constant, positive
/// duration, but no multiple of a week or a month, which TQL buckets by the
/// calendar.
auto bucket_width(ast::expression const& expr) -> Option<duration>;

/// Returns the row of `rows` for `name` that applies to `args`, if any: one
/// with as many positional arguments, whose holes name exactly the named
/// arguments of the call.
auto find_row(std::span<Function const> rows, std::string_view name,
              std::span<ast::expression const> args) -> Function const*;

/// Returns the row of `rows` for `name` with `arity` positional arguments and
/// no named ones, if any.
auto find_row(std::span<Function const> rows, std::string_view name,
              size_t arity) -> Function const*;

/// How a non-constant hole in a spelling expects its argument to render.
enum class HoleKind { value, predicate, expr };

/// Escapes regex metacharacters in `text` so that a regex matches it literally.
auto escape_regex(std::string_view text) -> std::string;

/// Fills the holes of `spelling` for `args` with `fill`, which receives an
/// argument and its hole kind. Returns `None` if an argument does not fit its
/// hole, or if `fill` does.
auto spell(
  std::string_view spelling, std::span<ast::expression const> args,
  folly::FunctionRef<auto(ast::expression const&, HoleKind)->Option<std::string>>
    fill) -> Option<std::string>;

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

  /// Spells a finite `double`, or returns `None` if the target cannot.
  virtual auto render_double(double value) const -> Option<std::string> = 0;

  /// Spells a string literal, or returns `None` if the dialect cannot spell
  /// `text`.
  virtual auto quote_string(std::string_view text) const -> Option<std::string>
    = 0;

  /// Spells a time, a duration, or an IP address literal. Each defaults to
  /// vetoing.
  virtual auto render_time(time value) const -> Option<std::string>;
  virtual auto render_duration(duration value) const -> Option<std::string>;
  virtual auto render_ip(ip value) const -> Option<std::string>;

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

  /// How the value of `expr` decodes when the target computes it, or `None`
  /// if the dialect cannot tell, which keeps a group key of it local.
  /// Defaults to the row of `functions()` of a call, and natively otherwise.
  virtual auto value_decode(ast::expression const& expr) const
    -> Option<Decode>;

  /// Whether a boolean value may stand where the dialect expects a predicate,
  /// as in `WHERE flag`, and a predicate where it expects a value, as in
  /// `(x > 1) = TRUE`.
  virtual auto boolean_values() const -> bool = 0;

  /// Spells a predicate as a value when `boolean_values()` is `false`, or
  /// returns `None` if the dialect cannot. Defaults to vetoing.
  virtual auto render_predicate_value(std::string_view predicate) const
    -> Option<std::string>;
};

/// How the result column of `column` of an aggregation decodes, or `None` to
/// keep it local. A group key decodes as its value, which `decode_of` tells,
/// and so does an extremum where its value does not decode natively. Every
/// other function decodes as its row of `ExpressionDialect::aggregations`.
auto aggregate_decode(
  ExpressionDialect const& dialect, ir::AggregateColumn const& column,
  folly::FunctionRef<auto(ast::expression const&)->Option<Decode>> decode_of)
  -> Option<Decode>;

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
    return pushdown::spell(
      row.spelling, args, [&](ast::expression const& arg, HoleKind kind) {
        return kind == HoleKind::predicate ? this->predicate(arg) : value(arg);
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
