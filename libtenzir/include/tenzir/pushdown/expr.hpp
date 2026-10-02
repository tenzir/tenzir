//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/pushdown/column_model.hpp"
#include "tenzir/variant.hpp"

#include <cstdint>
#include <string>
#include <vector>

/// The intermediate representation of pushed filters.
///
/// Translation turns a TQL predicate into an `Expr` that keeps and drops the
/// same rows, except where an operation says that it approximates TQL, such as
/// `Operation::fold_case`, and a source's renderer spells it in the target's
/// query language. The IR names
/// what to compute, never how to spell it: `WHERE x > 1`, `| where x > 1`, and
/// `Table.SelectRows(t, each [x] > 1)` are all renderings of `Binary{gt,
/// Column{x}, Literal{1}}`.
///
/// An `Expr` evaluates with SQL's three-valued logic. Comparisons, arithmetic,
/// `In`, `Between`, and operations yield `NULL` when an operand is `NULL`;
/// `And`, `Or`, and `Not` follow Kleene logic; `IsNull` is always `true` or
/// `false`. Where TQL treats `null` differently, translation spells that out
/// with explicit guards. Floating-point comparisons follow IEEE 754, so `NaN`
/// equals and orders with no value, itself included; `Binary::may_be_nan`
/// marks the comparisons where that matters. A renderer must preserve these
/// semantics where its target differs, or veto.
namespace tenzir::pushdown {

struct Expr;

/// The `NULL` constant.
struct Null {};

/// An element name of an enum column, exactly as TQL sees it.
struct EnumLabel {
  std::string name;
};

/// An instant on the tick grid of a temporal column, in the column's own type.
/// The tick lies within `[type.lo, type.hi]`.
struct TimeValue {
  int64_t ticks;
  TimeType type;
};

/// An IP address in the family of an IP column. If `v4` is set, the address
/// is an IPv4 address.
struct IpValue {
  ip address;
  bool v4;
};

/// A constant. A `double` is always finite: TQL has no syntax for other
/// values, and translation rejects them.
struct Literal {
  variant<Null, bool, int64_t, uint64_t, double, std::string, EnumLabel,
          TimeValue, IpValue>
    value;
};

/// A column, or a field nested in one.
struct Column {
  std::vector<std::string> path;
};

/// A function whose meaning does not depend on the target.
enum class Operation {
  /// `starts_with(string, prefix) -> bool`
  starts_with,
  /// `ends_with(string, suffix) -> bool`
  ends_with,
  /// `contains(string, needle) -> bool`, a byte-wise substring search that
  /// finds the empty needle in every string.
  contains,
  /// `length_bytes(string) -> uint64`, counting padding bytes.
  length_bytes,
  /// `parse_ip(string) -> ip`, an IPv6 address with IPv4 mapped into it, or
  /// `NULL` if the target cannot parse the string.
  parse_ip,
  /// `fold_case(string) -> string`, the caseless form of a string. TQL
  /// applies Unicode's full case folding, which maps `ß` to `ss`, and a target
  /// may approximate it with its lowercase mapping, which agrees on ASCII and
  /// on most other text. Unlike every other node, this one need not match TQL
  /// exactly: translation emits it only where the user asked to ignore case,
  /// and accepts the difference there.
  fold_case,
  /// `match_regex(string, pattern) -> bool`, whether RE2 with its default
  /// options finds a match of `pattern` anywhere in the string. `pattern` is a
  /// string literal that RE2 accepts. With these options, `.` does not match a
  /// newline, and `^` and `$` match only at the ends of the string. Like
  /// `fold_case`, this one need not match TQL exactly: a target that builds
  /// on its own release of RE2 may disagree on rare syntax, and a target may
  /// match invalid UTF-8 differently. Translation accepts these differences.
  match_regex,
};

/// A call of an operation.
struct Call {
  Operation op;
  std::vector<Expr> args;
};

/// The operators of `Binary`.
///
/// Comparisons yield a boolean. Arithmetic has a fixed result type, which
/// translation relies on: on two integers, it yields the exact result as a
/// 64-bit integer, which never overflows because translation only emits
/// integer arithmetic whose result fits; on any other operands, and for `div`
/// always, it converts both operands to `double` and yields a `double` with
/// IEEE 754 rounding.
///
/// A target that computes in the types of the operands, as DuckDB does for
/// `INTEGER + 1` and `FLOAT + 1.0`, does not deliver these result types by
/// itself: the former overflows past 32 bits, and the latter rounds to single
/// precision. Its renderer must spell arithmetic so that it delivers them, for
/// example by widening the operands to `Binary::type`, or veto it. A target
/// that yields the exact integer result in a narrower type that holds it
/// delivers them too.
enum class BinaryOp {
  eq,
  neq,
  lt,
  leq,
  gt,
  geq,
  add,
  sub,
  mul,
  div,
};

/// The result type of arithmetic, as `BinaryOp` defines it.
enum class ArithmeticType {
  /// The exact result as a 64-bit integer.
  integer,
  /// The result in `double`, with IEEE 754 rounding.
  floating,
};

/// A comparison or an arithmetic operation.
struct Binary {
  BinaryOp op;
  Box<Expr> left;
  Box<Expr> right;
  /// Whether an operand of a comparison may be `NaN`, which is meaningful
  /// only for the comparison operators. `false` guarantees that neither
  /// operand can be `NaN`. `true` marks every comparison with a
  /// floating-point operand other than a literal, whether a column or a value
  /// computed from one, such as `x * 0.5`, and whether or not that operand
  /// can actually be `NaN`. A target that equates or orders `NaN`, such as
  /// DuckDB, must guard or veto such a comparison. `In` needs no such mark:
  /// its elements are literals, which are never `NaN`, and `NaN` equals no
  /// other value in any target.
  bool may_be_nan = false;
  /// The result type of arithmetic, which is meaningful only for the
  /// arithmetic operators. Translation derives it from the operand types,
  /// which a renderer does not see, so that a dialect that computes in the
  /// operand types can widen them to it.
  ArithmeticType type = ArithmeticType::integer;
};

/// Whether all of at least two operands are `true`.
struct And {
  std::vector<Expr> operands;
};

/// Whether any of at least two operands is `true`.
struct Or {
  std::vector<Expr> operands;
};

/// The negation of a boolean.
struct Not {
  Box<Expr> expr;
};

/// Whether `expr` equals an element of `list`, whose elements are literals
/// other than `Null`.
struct In {
  Box<Expr> expr;
  std::vector<Expr> list;
};

/// Whether `expr` lies within `[lo, hi]`.
struct Between {
  Box<Expr> expr;
  Box<Expr> lo;
  Box<Expr> hi;
};

/// Whether `expr` is `NULL`, or is not if `negated` is set.
struct IsNull {
  Box<Expr> expr;
  bool negated = false;
};

/// `then` where `condition` is `true`, and `otherwise` where it is `false` or
/// `NULL`.
struct Conditional {
  Box<Expr> condition;
  Box<Expr> then;
  Box<Expr> otherwise;
};

/// An expression of the IR.
struct Expr : variant<Literal, Column, Call, Binary, And, Or, Not, In, Between,
                      IsNull, Conditional> {
  using variant::variant;
};

} // namespace tenzir::pushdown
