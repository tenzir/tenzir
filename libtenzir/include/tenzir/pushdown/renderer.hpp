//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"
#include "tenzir/pushdown/expr.hpp"

#include <span>
#include <string>
#include <string_view>

namespace tenzir::pushdown {

/// Spells IR expressions in a target's query language.
///
/// Translation decides whether a predicate has an exact counterpart in the IR,
/// and the renderer whether the target can spell it. A predicate is pushed only
/// if both agree. A target without an address type, for example, vetoes every
/// `IpValue`, and predicates on addresses stay local.
///
/// SQL dialects derive from `SqlRenderer`. Targets that are not SQL, such as
/// Power Query or KQL, derive from `Renderer` directly and apply their own
/// grouping rules.
class Renderer {
public:
  virtual ~Renderer() = default;

  /// Renders `expr`, or returns `None` if the target cannot spell some part of
  /// it.
  virtual auto render(Expr const& expr) const -> Option<std::string> = 0;
};

/// The part of rendering that SQL dialects share.
///
/// Spells comparisons, arithmetic, `AND`, `OR`, `NOT`, `IS NULL`, `IN`, and
/// `BETWEEN`, and leaves identifiers, literals, operations, and conditionals to
/// the dialect.
///
/// Hooks exchange `Fragment`s: rendered text together with its syntactic kind.
/// Only this class places grouping parentheses, based on a fragment's kind and
/// the slot it fills in the enclosing text. A dialect never writes one: it
/// composes text with the `sql_*` builders, which group each child as needed.
/// The rendered expression as a whole fills a logical slot, so a rendered
/// junction is grouped and joins other predicates with `AND` as is.
class SqlRenderer : public Renderer {
public:
  auto render(Expr const& expr) const -> Option<std::string> final;

protected:
  /// Rendered text, together with the syntactic kind that decides where it
  /// needs grouping.
  struct Fragment {
    /// What decides grouping is the syntax of a fragment, never its type.
    enum class Kind {
      /// Text that delimits itself and is never grouped: a literal, a column,
      /// a function call, or `CASE ... END`. `startsWith(x, 'a')` is an atom,
      /// although it yields a boolean.
      atom,
      /// A comparison, `NOT`, `IS NULL`, `IN`, or `BETWEEN`, including an
      /// operation spelled as one, such as `position(x, 'a') > 0`. Grouped as
      /// an operand of a predicate or of arithmetic, since dialects disagree
      /// on how these bind among themselves, but not as an operand of `AND`,
      /// `OR`, or `NOT`.
      predicate,
      /// `AND` or `OR`. Grouped everywhere except in a delimited slot, even
      /// as an operand of a junction with the same keyword, so that the text
      /// keeps the grouping of the IR.
      junction,
      /// An arithmetic operation. Grouped everywhere except in a delimited
      /// slot.
      arithmetic,
    };

    std::string text;
    Kind kind;
  };

  // -- Lexical components -----------------------------------------------------

  /// Quotes one segment of a column path. Segments are joined with `.`.
  virtual auto quote_identifier(std::string_view name) const -> std::string = 0;

  /// Quotes a string literal.
  virtual auto quote_string(std::string_view text) const -> std::string = 0;

  /// Spells a finite `double` so that the target reads it as a floating-point
  /// number.
  virtual auto render_double(double x) const -> std::string = 0;

  // -- Expressions ------------------------------------------------------------

  /// Spells a string literal, or vetoes it if the target cannot spell some
  /// of its bytes. Defaults to `quote_string`.
  virtual auto render_string(std::string_view text) const -> Option<Fragment>;

  /// Spells an enum label. Vetoes by default.
  virtual auto render_enum(EnumLabel const& x) const -> Option<Fragment>;

  /// Spells a time literal in its column's type. Vetoes by default.
  virtual auto render_time(TimeValue const& x) const -> Option<Fragment>;

  /// Spells an IP literal in its column's family. Vetoes by default.
  virtual auto render_ip(IpValue const& x) const -> Option<Fragment>;

  /// Spells a call of `op` on its rendered `args`. A predicate operation may
  /// be spelled as a comparison, such as `position(x, 'a') > 0` with
  /// `sql_binary`. Vetoes by default.
  virtual auto render_call(Operation op, std::span<Fragment const> args) const
    -> Option<Fragment>;

  /// Spells the comparison `x` on its rendered operands, or vetoes it. A
  /// dialect whose `NaN` semantics differ from IEEE 754 guards comparisons
  /// with `x.may_be_nan` here, which may make the result a junction, or
  /// vetoes them. Defaults to `left op right`.
  virtual auto render_comparison(Binary const& x, Fragment const& left,
                                 Fragment const& right) const
    -> Option<Fragment>;

  /// Spells the arithmetic `x` on its rendered operands, or vetoes it. The
  /// spelling must deliver the result type that `BinaryOp` defines, for
  /// example by widening a narrower operand; a dialect that cannot vetoes.
  /// Defaults to `left op right`, which suits a target that widens by itself.
  virtual auto render_arithmetic(Binary const& x, Fragment const& left,
                                 Fragment const& right) const
    -> Option<Fragment>;

  /// Spells a conditional on its rendered operands, or vetoes it. Defaults to
  /// `CASE WHEN`.
  virtual auto
  render_conditional(Fragment const& condition, Fragment const& then,
                     Fragment const& otherwise) const -> Option<Fragment>;

  // -- Builders ---------------------------------------------------------------

  /// Makes an atom of text that delimits itself, such as a number or a quoted
  /// string.
  auto sql_atom(std::string text) const -> Fragment;

  /// Spells the call `name(args...)`, an atom.
  auto sql_call(std::string_view name, std::span<Fragment const> args) const
    -> Fragment;

  /// Spells `left op right` for the infix operator `op`. `kind` is the kind of
  /// the result, either `predicate` or `arithmetic`.
  auto sql_binary(std::string_view op, Fragment const& left,
                  Fragment const& right, Fragment::Kind kind) const -> Fragment;

  /// Joins at least two `operands` with `keyword`, such as `AND` or `OR`.
  auto sql_junction(std::string_view keyword,
                    std::span<Fragment const> operands) const -> Fragment;

  /// Spells `NOT operand`, a predicate.
  auto sql_not(Fragment const& operand) const -> Fragment;

  /// Spells `CASE WHEN condition THEN then ELSE otherwise END`, an atom. A
  /// dialect with a conditional function spells it with `sql_call` instead.
  auto sql_conditional(Fragment const& condition, Fragment const& then,
                       Fragment const& otherwise) const -> Fragment;

private:
  /// The place of a fragment in the text that encloses it.
  enum class Slot {
    /// Delimited by a list, such as the arguments of a function or the
    /// elements of `IN`, or by keywords, such as `CASE WHEN ... THEN`.
    delimited,
    /// An operand of `AND`, `OR`, or `NOT`, or the whole expression.
    logical,
    /// An operand of a comparison, arithmetic, `IS NULL`, `IN`, or `BETWEEN`.
    operand,
  };

  /// Returns the text of `child`, grouped if its kind requires it in `slot`.
  auto wrap(Fragment const& child, Slot slot) const -> std::string;

  auto render_fragment(Expr const& expr) const -> Option<Fragment>;

  auto render_literal(Literal const& x) const -> Option<Fragment>;
};

} // namespace tenzir::pushdown
