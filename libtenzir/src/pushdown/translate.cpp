//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/translate.hpp"

#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/uuid.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <concepts>
#include <limits>

namespace tenzir::pushdown {

namespace {

// -- IR construction ----------------------------------------------------------

/// A literal holding `x`.
template <class T>
auto lit(T x) -> Expr {
  return Literal{std::move(x)};
}

/// Collects `xs` into a list by moving them, where an initializer list would
/// copy.
template <class... Ts>
auto exprs(Ts... xs) -> std::vector<Expr> {
  auto result = std::vector<Expr>{};
  result.reserve(sizeof...(Ts));
  (result.emplace_back(std::move(xs)), ...);
  return result;
}

auto binary(BinaryOp op, Expr left, Expr right, bool may_be_nan = false)
  -> Expr {
  return Binary{op, std::move(left), std::move(right), may_be_nan};
}

auto conjunction(Expr left, Expr right) -> Expr {
  return And{exprs(std::move(left), std::move(right))};
}

auto disjunction(Expr left, Expr right) -> Expr {
  return Or{exprs(std::move(left), std::move(right))};
}

/// The conjunction of `parts`, which must not be empty.
auto conjunction(std::vector<Expr> parts) -> Expr {
  TENZIR_ASSERT(not parts.empty());
  if (parts.size() == 1) {
    return std::move(parts.front());
  }
  return And{std::move(parts)};
}

auto is_null(Expr x) -> Expr {
  return IsNull{std::move(x), false};
}

auto is_not_null(Expr x) -> Expr {
  return IsNull{std::move(x), true};
}

template <class... Ts>
auto call(Operation op, Ts... args) -> Expr {
  return Call{op, exprs(std::move(args)...)};
}

// -- Numbers ------------------------------------------------------------------

/// The smallest integer magnitude that a `double` cannot represent exactly.
constexpr auto exact_double_bound = 9007199254740992.0;

/// Returns whether `x` converts to `double` and back without loss.
template <class T>
  requires std::integral<T>
auto is_exact_in_double(T x) -> bool {
  auto d = static_cast<double>(x);
  // Guard the conversion back: `double` rounds the extremes of the range up
  // to a power of two that `T` cannot hold.
  if (d >= static_cast<double>(std::numeric_limits<T>::max())
      or d < static_cast<double>(std::numeric_limits<T>::min())) {
    return false;
  }
  return static_cast<T>(d) == x;
}

/// Returns whether comparing a numeric column with `literal` yields the same
/// result in TQL and the target.
///
/// Same-kind comparisons always do. Mixed comparisons differ in how they are
/// evaluated: TQL converts the integer side to `double` and compares doubles,
/// while a target may compare the two numbers exactly. When the integer side
/// is the literal, both agree as long as the literal converts exactly. When
/// the integer side is the column, its values are what gets rounded, and both
/// agree as long as the literal stays below `exact_double_bound`: a column
/// value at or beyond it then rounds to a `double` on the same side of the
/// literal.
auto numeric_literal_fits(bool integer_column, ast::constant const& literal)
  -> bool {
  return match(
    literal.value,
    [&](int64_t x) {
      return integer_column or is_exact_in_double(x);
    },
    [&](uint64_t x) {
      return integer_column or is_exact_in_double(x);
    },
    [&](double x) {
      // TQL has no syntax for non-finite literals; reject them defensively.
      if (not std::isfinite(x)) {
        return false;
      }
      return not integer_column or std::fabs(x) < exact_double_bound;
    },
    [](auto const&) {
      return false;
    });
}

/// Converts a boolean, numeric, or string constant into a literal.
auto to_literal(ast::constant const& constant) -> Expr {
  return match(
    constant.value,
    [](bool x) {
      return lit(x);
    },
    [](int64_t x) {
      return lit(x);
    },
    [](uint64_t x) {
      return lit(x);
    },
    [](double x) {
      return lit(x);
    },
    [](std::string const& x) {
      return lit(x);
    },
    [](auto const&) -> Expr {
      TENZIR_UNREACHABLE();
    });
}

// -- Literals -----------------------------------------------------------------

/// Returns whether `expr` consists of constants combined by operators and
/// deterministic functions only, so that it can be evaluated ahead of time.
auto is_literal_tree(ast::expression const& expr, registry const& reg) -> bool {
  return match(
    expr,
    [](ast::constant const&) {
      return true;
    },
    [&](ast::unary_expr const& x) {
      return is_literal_tree(x.expr, reg);
    },
    [&](ast::binary_expr const& x) {
      return is_literal_tree(x.left, reg) and is_literal_tree(x.right, reg);
    },
    [&](ast::function_call const& x) {
      return reg.get(x).is_deterministic()
             and std::ranges::all_of(x.args, [&](ast::expression const& arg) {
                   return is_literal_tree(arg, reg);
                 });
    },
    [](auto const&) {
      return false;
    });
}

/// Returns `expr` as a constant if it is one or folds to one. Folding uses
/// TQL's own evaluator, so `2024-01-01 + 1d` and `-5` become the constants
/// that TQL would compute at runtime. An expression whose evaluation warns or
/// yields `null`, such as an overflowing product, is not folded: it stays
/// local, where the warning reaches the user.
auto as_literal(ast::expression const& expr) -> Option<ast::constant> {
  if (auto const* constant = try_as<ast::constant>(expr)) {
    return *constant;
  }
  auto reg = global_registry();
  if (not reg or not is_literal_tree(expr, *reg)) {
    return None{};
  }
  auto dh = collecting_diagnostic_handler{};
  auto result = const_eval(expr, dh);
  if (not result or not std::move(dh).collect().empty()
      or is<caf::none_t>(result->inner)) {
    return None{};
  }
  return ast::constant::make(std::move(*result));
}

auto comparison_op(ast::binary_op op) -> BinaryOp {
  switch (op) {
    case ast::binary_op::eq:
      return BinaryOp::eq;
    case ast::binary_op::neq:
      return BinaryOp::neq;
    case ast::binary_op::lt:
      return BinaryOp::lt;
    case ast::binary_op::leq:
      return BinaryOp::leq;
    case ast::binary_op::gt:
      return BinaryOp::gt;
    case ast::binary_op::geq:
      return BinaryOp::geq;
    default:
      TENZIR_UNREACHABLE();
  }
}

/// Mirrors `op` so that the column ends up on the left-hand side.
auto flip_comparison(ast::binary_op op) -> ast::binary_op {
  switch (op) {
    case ast::binary_op::lt:
      return ast::binary_op::gt;
    case ast::binary_op::leq:
      return ast::binary_op::geq;
    case ast::binary_op::gt:
      return ast::binary_op::lt;
    case ast::binary_op::geq:
      return ast::binary_op::leq;
    default:
      return op;
  }
}

auto is_equality(ast::binary_op op) -> bool {
  return op == ast::binary_op::eq or op == ast::binary_op::neq;
}

auto is_less(ast::binary_op op) -> bool {
  return op == ast::binary_op::lt or op == ast::binary_op::leq;
}

/// Compares `x` with `y`. `may_be_nan` tells whether one of them is a
/// floating-point value other than a literal.
auto compare(Expr x, ast::binary_op op, Expr y, bool may_be_nan = false)
  -> Expr {
  return binary(comparison_op(op), std::move(x), std::move(y), may_be_nan);
}

/// Translates `x == literal` or `x != literal` with TQL's null semantics,
/// where `null` never equals a value.
auto equality(Expr x, bool nullable, ast::binary_op op, Expr literal,
              bool may_be_nan = false) -> Expr {
  if (not nullable) {
    return compare(std::move(x), op, std::move(literal), may_be_nan);
  }
  auto comparison = compare(x, op, std::move(literal), may_be_nan);
  if (op == ast::binary_op::eq) {
    return conjunction(is_not_null(std::move(x)), std::move(comparison));
  }
  return disjunction(is_null(std::move(x)), std::move(comparison));
}

/// The outcome of an equality whose literal can never match, such as an
/// unknown enum name. TQL yields `false` for every row, `null` included, and
/// `!=` yields `true`.
auto never_equal(ast::binary_op op) -> Expr {
  return lit(op != ast::binary_op::eq);
}

// -- Time ---------------------------------------------------------------------

/// Divides rounding toward negative infinity. Requires `divisor > 0`.
auto floor_div(int64_t dividend, int64_t divisor)
  -> std::pair<int64_t, int64_t> {
  auto quotient = dividend / divisor;
  auto remainder = dividend % divisor;
  if (remainder < 0) {
    --quotient;
    remainder += divisor;
  }
  return {quotient, remainder};
}

/// The literal for `ticks` in the column's type. Requires `ticks` to lie
/// within `[lo, hi]`.
auto time_literal(TimeType const& type, int64_t ticks) -> Expr {
  TENZIR_ASSERT(ticks >= type.lo and ticks <= type.hi);
  return lit(TimeValue{.ticks = ticks, .type = type});
}

/// The conditions under which TQL sees a value rather than `null` for the
/// temporal column `x`. None are needed if it always does.
auto time_guard(Expr const& x, TimeType const& type) -> std::vector<Expr> {
  auto result = std::vector<Expr>{};
  if (type.guard_lo) {
    result.push_back(binary(BinaryOp::geq, x, time_literal(type, type.lo)));
  }
  if (type.guard_hi) {
    result.push_back(binary(BinaryOp::leq, x, time_literal(type, type.hi)));
  }
  return result;
}

/// The temporal column `x` as TQL sees it: `NULL` where the stored tick does
/// not decode.
auto time_view(Expr x, TimeType const& type) -> Expr {
  auto guard = time_guard(x, type);
  if (guard.empty()) {
    return x;
  }
  return Conditional{conjunction(std::move(guard)), std::move(x), lit(Null{})};
}

/// Returns the tick of `t` if `t` lies on the grid and within range.
auto exact_tick(TimeType const& type, time t) -> Option<int64_t> {
  auto [ticks, remainder] = floor_div(t.time_since_epoch().count(), type.unit);
  if (remainder != 0 or ticks < type.lo or ticks > type.hi) {
    return None{};
  }
  return ticks;
}

/// Translates a comparison of a temporal column with a `time` literal.
///
/// TQL compares nanoseconds. A literal between two ticks or outside the
/// column's range therefore has a fixed relation to every stored value, which
/// the translation resolves: equality becomes a constant, and an ordering
/// moves to the nearest tick or clamps to the range's edge. The clamped forms
/// compare the column against its own minimum or maximum, which keeps the
/// result `null` for `null` rows the way TQL does.
///
/// Rows that TQL decodes to `null` must order as `null`. In a `positive`
/// context, where `false` and `null` both drop the row, the guard joins the
/// comparison with `and`, which leaves it analyzable for the primary key. That
/// yields `false` where TQL yields `null`, which has the same keep-or-drop
/// outcome. Elsewhere the comparison yields `NULL` outright.
auto translate_time_comparison(Expr x, bool nullable, TimeType const& type,
                               ast::binary_op op, time literal, bool positive)
  -> Expr {
  if (is_equality(op)) {
    auto ticks = exact_tick(type, literal);
    if (not ticks) {
      return never_equal(op);
    }
    return equality(std::move(x), nullable, op, time_literal(type, *ticks));
  }
  auto [ticks, remainder]
    = floor_div(literal.time_since_epoch().count(), type.unit);
  if (remainder != 0) {
    // With `t` strictly between the ticks `g` and `g + 1`, `x < t` holds
    // exactly when `x <= g`, and `x >= t` exactly when `x > g`.
    if (op == ast::binary_op::lt) {
      op = ast::binary_op::leq;
    } else if (op == ast::binary_op::geq) {
      op = ast::binary_op::gt;
    }
  }
  if (ticks < type.lo) {
    // Every stored value is greater than the literal.
    op = is_less(op) ? ast::binary_op::lt : ast::binary_op::geq;
    ticks = type.lo;
  } else if (ticks > type.hi) {
    // Every stored value is less than the literal.
    op = is_less(op) ? ast::binary_op::leq : ast::binary_op::gt;
    ticks = type.hi;
  }
  auto guard = time_guard(x, type);
  auto comparison = compare(std::move(x), op, time_literal(type, ticks));
  if (guard.empty()) {
    return comparison;
  }
  if (positive) {
    guard.push_back(std::move(comparison));
    return And{std::move(guard)};
  }
  return Conditional{conjunction(std::move(guard)), std::move(comparison),
                     lit(Null{})};
}

// -- IP addresses -------------------------------------------------------------

/// The IPv4-mapped range that an IPv4 column covers in TQL's 16-byte address
/// space.
auto v4_range() -> std::pair<ip, ip> {
  return {ip::v4(uint32_t{0}), ip::v4(uint32_t{0xffffffff})};
}

/// The literal for `x` in the column's family.
auto ip_literal(ip const& x, bool v4) -> Expr {
  TENZIR_ASSERT(not v4 or x.is_v4());
  return lit(IpValue{.address = x, .v4 = v4});
}

/// The last address of `sn`: its network with every host bit set.
auto subnet_end(subnet const& sn) -> ip {
  auto all_ones = std::array<uint8_t, 16>{};
  all_ones.fill(0xff);
  auto host_mask = ip{all_ones};
  auto net_mask = ip{all_ones};
  net_mask.mask(sn.length());
  host_mask ^= net_mask;
  auto end = sn.network();
  end |= host_mask;
  return end;
}

/// Translates a comparison of an IP column with an `ip` literal.
///
/// TQL compares the 16-byte forms, with IPv4 addresses mapped into IPv6. The
/// IR orders both families the same way, so a literal of the column's family
/// translates directly. An IPv6 literal against an IPv4 column lies entirely
/// below or above the mapped range, which fixes the outcome; the ordering
/// forms clamp to the range's edge to stay `null` for `null` rows.
auto translate_ip_comparison(Expr x, bool nullable, IpType type,
                             ast::binary_op op, ip const& literal) -> Expr {
  if (type.v4 and not literal.is_v4()) {
    if (is_equality(op)) {
      return never_equal(op);
    }
    auto [lo, hi] = v4_range();
    auto below = literal < lo;
    op = below ? (is_less(op) ? ast::binary_op::lt : ast::binary_op::geq)
               : (is_less(op) ? ast::binary_op::leq : ast::binary_op::gt);
    return compare(std::move(x), op, ip_literal(below ? lo : hi, true));
  }
  auto rendered = ip_literal(literal, type.v4);
  if (is_equality(op)) {
    return equality(std::move(x), nullable, op, std::move(rendered));
  }
  return compare(std::move(x), op, std::move(rendered));
}

/// Translates `column in subnet` into a range check. Both TQL and the IR
/// yield `null` for a `null` address, so no guard is needed, and a range that
/// misses an IPv4 column entirely becomes a comparison that is `false` for
/// every address and `null` for none.
auto translate_ip_in_subnet(Expr x, IpType type, subnet const& sn) -> Expr {
  auto lo = sn.network();
  auto hi = subnet_end(sn);
  if (type.v4) {
    auto [v4_lo, v4_hi] = v4_range();
    if (hi < v4_lo or lo > v4_hi) {
      return binary(BinaryOp::lt, std::move(x), ip_literal(v4_lo, true));
    }
    lo = std::max(lo, v4_lo);
    hi = std::min(hi, v4_hi);
  }
  return Between{std::move(x), ip_literal(lo, type.v4),
                 ip_literal(hi, type.v4)};
}

// -- Text-like types ----------------------------------------------------------

/// Returns the literal that compares equal to `text` in a fixed-length string
/// column, or `None` if no stored value can. TQL sees all stored bytes,
/// padding included, so only a literal of exactly that length can equal one.
/// A target may pad a shorter literal for the comparison, which would match
/// where TQL does not.
auto fixed_string_literal(std::string const& text, FixedStringType const& type)
  -> Option<Expr> {
  if (text.size() != type.length) {
    return None{};
  }
  return lit(text);
}

/// Returns the literal for an enum name, or `None` if the enum has no such
/// element. A target may reject or never match an unknown name; TQL never
/// matches it.
auto enum_literal(std::string const& text, EnumType const& type)
  -> Option<Expr> {
  if (not type.names.contains(text)) {
    return None{};
  }
  return lit(EnumLabel{text});
}

/// Returns the literal for a UUID, or `None` if `text` is not in the canonical
/// lowercase form that TQL sees. A target may parse uppercase digits too, so
/// an uppercase literal would match where TQL does not; parsing and printing
/// the text back tells the two apart.
auto uuid_literal(std::string const& text) -> Option<Expr> {
  auto parsed = uuid{};
  if (not parsers::uuid(text, parsed) or fmt::to_string(parsed) != text) {
    return None{};
  }
  return lit(text);
}

// -- Scalars ------------------------------------------------------------------

/// A translated value expression: a column, or arithmetic on one.
struct Scalar {
  Expr expr;
  ColumnType type;
  bool nullable;
};

/// The expression of `scalar` as TQL sees it: `NULL` where a temporal value
/// does not decode.
auto tql_view(Scalar const& scalar) -> Expr {
  if (auto const* type = try_as<TimeType>(scalar.type)) {
    return time_view(scalar.expr, *type);
  }
  return scalar.expr;
}

/// Resolves a field path expression to a column.
auto resolve_column(ast::expression const& expr, ColumnModel const& columns)
  -> Option<Scalar> {
  auto path = ast::field_path::try_from(expr);
  if (not path or path->path().empty()) {
    return None{};
  }
  auto segments = std::vector<std::string>{};
  for (auto const& segment : path->path()) {
    segments.push_back(segment.id.name);
  }
  auto column = columns.find(segments);
  if (not column) {
    return None{};
  }
  return Scalar{.expr = Column{std::move(segments)},
                .type = column->type,
                .nullable = column->nullable};
}

/// Returns the name of a plain function call, such as `starts_with` for both
/// `starts_with(x, y)` and `x.starts_with(y)`.
auto function_name(ast::function_call const& call) -> Option<std::string_view> {
  if (call.fn.path.size() != 1) {
    return None{};
  }
  return call.fn.path.front().name;
}

auto arithmetic_op(ast::binary_op op) -> BinaryOp {
  switch (op) {
    case ast::binary_op::add:
      return BinaryOp::add;
    case ast::binary_op::sub:
      return BinaryOp::sub;
    case ast::binary_op::mul:
      return BinaryOp::mul;
    case ast::binary_op::div:
      return BinaryOp::div;
    default:
      TENZIR_UNREACHABLE();
  }
}

/// Translates arithmetic between a numeric column and a literal.
///
/// Both sides compute in `double` when either operand is one, converting an
/// integer operand with the same IEEE 754 rounding, so floating-point
/// arithmetic translates as long as no division by zero is involved: TQL
/// yields `null` for it, a target an infinity or an error. Division always
/// computes in `double`. Integer arithmetic differs on overflow, where TQL
/// yields `null` and a target may wrap around. It is only pushed when overflow
/// is impossible: on columns of at most 32 bits, with a literal below 2^31, so
/// that the result fits comfortably into 64 bits on both sides. Unsigned
/// columns yield an unsigned result in TQL, so only literals that cannot take
/// the result below zero are allowed.
auto translate_arithmetic(ast::binary_expr const& expr,
                          ColumnModel const& columns) -> Option<Scalar> {
  auto op = expr.op;
  auto column = resolve_column(expr.left, columns);
  auto literal = as_literal(expr.right);
  auto column_left = true;
  if (not column or not literal) {
    column = resolve_column(expr.right, columns);
    literal = as_literal(expr.left);
    column_left = false;
    // `literal / column` divides by zero wherever the column is zero.
    if (not column or not literal or op == ast::binary_op::div) {
      return None{};
    }
  }
  auto const* integer = try_as<IntType>(column->type);
  if (not integer and not is<FloatType>(column->type)) {
    return None{};
  }
  auto result_type = Option<ColumnType>{};
  match(
    literal->value,
    [&](double x) {
      if (std::isfinite(x) and (op != ast::binary_op::div or x != 0.0)) {
        result_type = FloatType{};
      }
    },
    [&](int64_t x) {
      if (op == ast::binary_op::div) {
        if (x != 0) {
          result_type = FloatType{};
        }
        return;
      }
      if (not integer) {
        result_type = FloatType{};
        return;
      }
      constexpr auto bound = int64_t{1} << 31;
      if (integer->bits > 32 or x <= -bound or x >= bound) {
        return;
      }
      if (not integer->is_signed) {
        // The result is unsigned in TQL, so the literal must not be able to
        // take it below zero. `literal - column` yields a signed result.
        auto column_minus = op == ast::binary_op::sub and column_left;
        auto column_plus = op != ast::binary_op::sub;
        if ((column_minus and x > 0) or (column_plus and x < 0)) {
          return;
        }
        if (op == ast::binary_op::sub and not column_left) {
          result_type = IntType{.bits = 64, .is_signed = true};
          return;
        }
      }
      result_type = IntType{.bits = 64, .is_signed = integer->is_signed};
    },
    [&](uint64_t) {
      // Only literals beyond the `int64` range are `uint64`: too large for
      // integer arithmetic, but a `double` operand like any other otherwise.
      if (not integer or op == ast::binary_op::div) {
        result_type = FloatType{};
      }
    },
    [](auto const&) {});
  if (not result_type) {
    return None{};
  }
  auto operand = to_literal(*literal);
  auto result = column_left ? binary(arithmetic_op(op), std::move(column->expr),
                                     std::move(operand))
                            : binary(arithmetic_op(op), std::move(operand),
                                     std::move(column->expr));
  return Scalar{.expr = std::move(result),
                .type = std::move(*result_type),
                .nullable = column->nullable};
}

/// Translates a function call that yields a scalar.
auto translate_scalar_function(ast::function_call const& call,
                               ColumnModel const& columns) -> Option<Scalar> {
  auto name = function_name(call);
  if (not name or call.args.size() != 1) {
    return None{};
  }
  if (*name == "length_bytes") {
    // Both count bytes, including the padding of a fixed-length string.
    auto column = resolve_column(call.args.front(), columns);
    if (not column
        or not(is<StringType>(column->type)
               or is<FixedStringType>(column->type))) {
      return None{};
    }
    return Scalar{.expr = pushdown::call(Operation::length_bytes,
                                         std::move(column->expr)),
                  .type = IntType{.bits = 64, .is_signed = false},
                  .nullable = column->nullable};
  }
  return None{};
}

auto translate_scalar(ast::expression const& expr, ColumnModel const& columns)
  -> Option<Scalar> {
  if (auto column = resolve_column(expr, columns)) {
    return column;
  }
  return match(
    expr,
    [&](ast::binary_expr const& x) -> Option<Scalar> {
      switch (x.op) {
        case ast::binary_op::add:
        case ast::binary_op::sub:
        case ast::binary_op::mul:
        case ast::binary_op::div:
          return translate_arithmetic(x, columns);
        default:
          return None{};
      }
    },
    [&](ast::function_call const& x) -> Option<Scalar> {
      return translate_scalar_function(x, columns);
    },
    [](auto const&) -> Option<Scalar> {
      return None{};
    });
}

// -- Comparisons --------------------------------------------------------------

/// Translates a comparison of a scalar with a literal that is not `null`.
/// `positive` tells whether `false` and `null` are interchangeable in the
/// surrounding predicate.
auto translate_literal_comparison(Scalar const& scalar, ast::binary_op op,
                                  ast::constant const& literal, bool positive)
  -> Option<Expr> {
  auto const* text = try_as<std::string>(literal.value);
  // Translates an equality against the literal of a text-like column, or the
  // constant outcome if no stored value can match.
  auto text_equality = [&](auto make_literal) -> Option<Expr> {
    if (not text or not is_equality(op)) {
      return None{};
    }
    auto converted = make_literal(*text);
    if (not converted) {
      return never_equal(op);
    }
    return equality(scalar.expr, scalar.nullable, op, std::move(*converted));
  };
  auto number = [&](bool integer) -> Option<Expr> {
    if (not numeric_literal_fits(integer, literal)) {
      return None{};
    }
    // Only a floating-point scalar can be `nan`; literals are finite.
    auto may_be_nan = not integer;
    if (is_equality(op)) {
      return equality(scalar.expr, scalar.nullable, op, to_literal(literal),
                      may_be_nan);
    }
    // Ordering yields `null` for `null` operands in both TQL and the IR, so
    // no guard is needed. Both sides agree on the order, including IEEE 754
    // semantics for `nan`, which neither of them orders.
    return compare(scalar.expr, op, to_literal(literal), may_be_nan);
  };
  return match(
    scalar.type,
    [&](BoolType const&) -> Option<Expr> {
      if (not is<bool>(literal.value) or not is_equality(op)) {
        return None{};
      }
      return equality(scalar.expr, scalar.nullable, op, to_literal(literal));
    },
    [&](IntType const&) -> Option<Expr> {
      return number(true);
    },
    [&](FloatType const&) -> Option<Expr> {
      return number(false);
    },
    [&](StringType const&) -> Option<Expr> {
      if (not text) {
        return None{};
      }
      if (is_equality(op)) {
        return equality(scalar.expr, scalar.nullable, op, lit(*text));
      }
      // Both compare bytes as unsigned values, shorter prefix first.
      return compare(scalar.expr, op, lit(*text));
    },
    [&](FixedStringType const& type) -> Option<Expr> {
      return text_equality([&](std::string const& x) {
        return fixed_string_literal(x, type);
      });
    },
    [&](EnumType const& type) -> Option<Expr> {
      return text_equality([&](std::string const& x) {
        return enum_literal(x, type);
      });
    },
    [&](UuidType const&) -> Option<Expr> {
      return text_equality(uuid_literal);
    },
    [&](TimeType const& type) -> Option<Expr> {
      auto const* t = try_as<time>(literal.value);
      if (not t) {
        return None{};
      }
      return translate_time_comparison(scalar.expr, scalar.nullable, type, op,
                                       *t, positive);
    },
    [&](IpType const& type) -> Option<Expr> {
      auto const* x = try_as<ip>(literal.value);
      if (not x) {
        return None{};
      }
      return translate_ip_comparison(scalar.expr, scalar.nullable, type, op,
                                     *x);
    });
}

/// Translates a comparison of two values with TQL's null semantics. TQL
/// treats `null` as a value for `==`, `!=`, `<=`, and `>=`: two nulls are
/// equal, and thus also less-or-equal, while `<` and `>` yield `null` like
/// the IR does. A `null` against a value is unequal and unordered on both
/// sides.
auto pairwise(Expr left, bool left_nullable, Expr right, bool right_nullable,
              ast::binary_op op, bool may_be_nan) -> Expr {
  auto both_null = [&] {
    return conjunction(is_null(left), is_null(right));
  };
  switch (op) {
    case ast::binary_op::eq:
    case ast::binary_op::neq: {
      if (not left_nullable and not right_nullable) {
        return compare(std::move(left), op, std::move(right), may_be_nan);
      }
      // Both sides `null` or both present and equal. Every part is a
      // definite boolean, so `not` on it stays definite.
      auto equality = Option<Expr>{};
      if (left_nullable and right_nullable) {
        auto nulls = both_null();
        auto present_left = is_not_null(left);
        auto present_right = is_not_null(right);
        auto equal
          = binary(BinaryOp::eq, std::move(left), std::move(right), may_be_nan);
        equality
          = disjunction(std::move(nulls),
                        And{exprs(std::move(present_left),
                                  std::move(present_right), std::move(equal))});
      } else {
        auto present = is_not_null(left_nullable ? left : right);
        auto equal
          = binary(BinaryOp::eq, std::move(left), std::move(right), may_be_nan);
        equality = conjunction(std::move(present), std::move(equal));
      }
      if (op == ast::binary_op::eq) {
        return std::move(*equality);
      }
      return Not{std::move(*equality)};
    }
    case ast::binary_op::leq:
    case ast::binary_op::geq:
      if (left_nullable and right_nullable) {
        auto nulls = both_null();
        return disjunction(std::move(nulls),
                           compare(std::move(left), op, std::move(right),
                                   may_be_nan));
      }
      return compare(std::move(left), op, std::move(right), may_be_nan);
    default:
      return compare(std::move(left), op, std::move(right), may_be_nan);
  }
}

/// Translates a comparison between two scalars.
///
/// Only same-kind pairs are pushed. TQL compares mixed integers and doubles
/// by converting the integer to `double`, which a target may not, and it
/// compares the textual form of enums, UUIDs, and fixed-length strings, where
/// a target pads or converts. Temporal columns must share their exact storage
/// type, since a target converts one side otherwise, and the conversion may
/// depend on the session time zone or overflow.
auto translate_pairwise_comparison(ast::binary_expr const& expr,
                                   ColumnModel const& columns) -> Option<Expr> {
  auto left = translate_scalar(expr.left, columns);
  if (not left) {
    return None{};
  }
  auto right = translate_scalar(expr.right, columns);
  if (not right) {
    return None{};
  }
  auto op = expr.op;
  auto left_expr = left->expr;
  auto right_expr = right->expr;
  auto left_nullable = left->nullable;
  auto right_nullable = right->nullable;
  auto may_be_nan = false;
  auto compatible = match(
    std::tie(left->type, right->type),
    [](IntType const&, IntType const&) {
      // Signed and unsigned integers compare exactly, in TQL and the IR.
      return true;
    },
    [&](FloatType const&, FloatType const&) {
      may_be_nan = true;
      return true;
    },
    [](StringType const&, StringType const&) {
      return true;
    },
    [](IpType const&, IpType const&) {
      // The IR maps IPv4 into IPv6 for mixed comparisons, as TQL does.
      return true;
    },
    [&](BoolType const&, BoolType const&) {
      return is_equality(op);
    },
    [&](UuidType const&, UuidType const&) {
      return is_equality(op);
    },
    [](FixedStringType const& x, FixedStringType const& y) {
      return x.length == y.length;
    },
    [&](TimeType const& x, TimeType const& y) {
      if (x != y) {
        return false;
      }
      // Values that TQL decodes to `null` must compare as `null`.
      if (x.guard_lo or x.guard_hi) {
        left_expr = tql_view(*left);
        right_expr = tql_view(*right);
        left_nullable = true;
        right_nullable = true;
      }
      return true;
    },
    [](auto const&, auto const&) {
      return false;
    });
  if (not compatible) {
    return None{};
  }
  return pairwise(std::move(left_expr), left_nullable, std::move(right_expr),
                  right_nullable, op, may_be_nan);
}

auto translate_comparison(ast::binary_expr const& expr,
                          ColumnModel const& columns, bool positive)
  -> Option<Expr> {
  // Exactly one side must be a literal; otherwise both must be scalars.
  auto op = expr.op;
  auto const* scalar_expr = &expr.left;
  auto literal = as_literal(expr.right);
  if (not literal) {
    literal = as_literal(expr.left);
    if (not literal) {
      return translate_pairwise_comparison(expr, columns);
    }
    scalar_expr = &expr.right;
    op = flip_comparison(op);
  }
  auto scalar = translate_scalar(*scalar_expr, columns);
  if (not scalar) {
    return None{};
  }
  // `x == null` and `x != null` are null checks in TQL, which also hold for
  // values that TQL cannot decode.
  if (is<caf::none_t>(literal->value)) {
    switch (op) {
      case ast::binary_op::eq:
        return is_null(tql_view(*scalar));
      case ast::binary_op::neq:
        return is_not_null(tql_view(*scalar));
      default:
        return None{};
    }
  }
  return translate_literal_comparison(*scalar, op, *literal, positive);
}

// -- Membership ---------------------------------------------------------------

/// Translates `scalar in [literals]`.
///
/// A TQL list has a single element type, fixed by its first element; a later
/// element of another type becomes `null` and never matches, even between
/// `int64` and `double` or `int64` and `uint64`. A target would match it, so
/// such lists stay local. So does a list with a `null`, which makes TQL and
/// the IR disagree for `null` rows. For the text-like and temporal types, an
/// element that no stored value can equal is dropped; when none remains, the
/// membership is `false` for every row, `null` included, as in TQL.
auto translate_in_list(Scalar const& scalar, ast::list const& list)
  -> Option<Expr> {
  if (list.items.empty()) {
    return None{};
  }
  auto constants = std::vector<ast::constant>{};
  auto element_type = Option<size_t>{};
  for (auto const& item : list.items) {
    auto const* item_expr = try_as<ast::expression>(item);
    if (not item_expr) {
      return None{};
    }
    auto constant = as_literal(*item_expr);
    if (not constant or is<caf::none_t>(constant->value)) {
      return None{};
    }
    auto type_index
      = variant_traits<ast::constant::kind>::index(constant->value);
    if (element_type and *element_type != type_index) {
      return None{};
    }
    element_type = type_index;
    constants.push_back(std::move(*constant));
  }
  auto literals = std::vector<Expr>{};
  // Collects the literals of a text-like type, dropping those that cannot
  // match.
  auto texts = [&](auto make_literal) {
    for (auto const& constant : constants) {
      auto const* text = try_as<std::string>(constant.value);
      if (not text) {
        return false;
      }
      if (auto converted = make_literal(*text)) {
        literals.push_back(std::move(*converted));
      }
    }
    return true;
  };
  auto ok = match(
    scalar.type,
    [&](BoolType const&) {
      for (auto const& constant : constants) {
        if (not is<bool>(constant.value)) {
          return false;
        }
        literals.push_back(to_literal(constant));
      }
      return true;
    },
    [&](IntType const&) {
      // Targets convert set elements to the column type and drop elements
      // that do not convert exactly, so `int_col IN (1.5)` never matches,
      // just like `x in [1.5]` in TQL.
      for (auto const& constant : constants) {
        if (not numeric_literal_fits(true, constant)) {
          return false;
        }
        literals.push_back(to_literal(constant));
      }
      return true;
    },
    [&](FloatType const&) {
      for (auto const& constant : constants) {
        if (not numeric_literal_fits(false, constant)) {
          return false;
        }
        literals.push_back(to_literal(constant));
      }
      return true;
    },
    [&](StringType const&) {
      return texts([](std::string const& x) -> Option<Expr> {
        return lit(x);
      });
    },
    [&](FixedStringType const& type) {
      return texts([&](std::string const& x) {
        return fixed_string_literal(x, type);
      });
    },
    [&](EnumType const& type) {
      return texts([&](std::string const& x) {
        return enum_literal(x, type);
      });
    },
    [&](UuidType const&) {
      return texts(uuid_literal);
    },
    [&](TimeType const& type) {
      for (auto const& constant : constants) {
        auto const* t = try_as<time>(constant.value);
        if (not t) {
          return false;
        }
        if (auto ticks = exact_tick(type, *t)) {
          literals.push_back(time_literal(type, *ticks));
        }
      }
      return true;
    },
    [&](IpType const& type) {
      for (auto const& constant : constants) {
        auto const* x = try_as<ip>(constant.value);
        if (not x) {
          return false;
        }
        if (not type.v4 or x->is_v4()) {
          literals.push_back(ip_literal(*x, type.v4));
        }
      }
      return true;
    });
  if (not ok) {
    return None{};
  }
  if (literals.empty()) {
    return lit(false);
  }
  auto membership = In{scalar.expr, std::move(literals)};
  if (scalar.nullable) {
    return conjunction(is_not_null(scalar.expr), std::move(membership));
  }
  return membership;
}

auto translate_in(ast::binary_expr const& expr, ColumnModel const& columns)
  -> Option<Expr> {
  if (auto const* list = try_as<ast::list>(expr.right)) {
    auto scalar = translate_scalar(expr.left, columns);
    if (not scalar) {
      return None{};
    }
    return translate_in_list(*scalar, *list);
  }
  if (auto constant = as_literal(expr.right)) {
    if (auto const* sn = try_as<subnet>(constant->value)) {
      auto column = resolve_column(expr.left, columns);
      auto const* type = column ? try_as<IpType>(column->type) : nullptr;
      if (not type) {
        return None{};
      }
      return translate_ip_in_subnet(std::move(column->expr), *type, *sn);
    }
    return None{};
  }
  // `"needle" in haystack` is a substring search on strings. Both sides
  // search bytes, find the empty needle everywhere, and yield `null` for a
  // `null` haystack.
  auto needle = as_literal(expr.left);
  auto const* text = needle ? try_as<std::string>(needle->value) : nullptr;
  if (not text) {
    return None{};
  }
  auto column = resolve_column(expr.right, columns);
  if (not column or not is<StringType>(column->type)) {
    return None{};
  }
  return call(Operation::contains, std::move(column->expr), lit(*text));
}

// -- Predicates ---------------------------------------------------------------

/// Translates a function call that yields a boolean.
auto translate_predicate_function(ast::function_call const& call,
                                  ColumnModel const& columns) -> Option<Expr> {
  auto name = function_name(call);
  if (not name) {
    return None{};
  }
  if (*name == "starts_with" or *name == "ends_with") {
    // The subject, the affix, and optionally `ignore_case`.
    if (call.args.size() < 2 or call.args.size() > 3) {
      return None{};
    }
    auto ignore_case = false;
    if (call.args.size() == 3) {
      auto const* named = try_as<ast::assignment>(call.args[2]);
      if (not named) {
        return None{};
      }
      auto label = ast::field_path::try_from(named->left);
      if (not label or label->path().size() != 1
          or label->path().front().id.name != "ignore_case") {
        return None{};
      }
      auto flag = as_literal(named->right);
      auto const* value = flag ? try_as<bool>(flag->value) : nullptr;
      if (not value) {
        return None{};
      }
      ignore_case = *value;
    }
    auto column = resolve_column(call.args[0], columns);
    if (not column or not is<StringType>(column->type)) {
      return None{};
    }
    auto prefix = as_literal(call.args[1]);
    auto const* text = prefix ? try_as<std::string>(prefix->value) : nullptr;
    if (not text) {
      return None{};
    }
    auto subject = std::move(column->expr);
    auto affix = lit(*text);
    if (ignore_case) {
      // The target folds both sides, so that the two foldings agree with each
      // other even where they differ from TQL's.
      subject = pushdown::call(Operation::fold_case, std::move(subject));
      affix = pushdown::call(Operation::fold_case, std::move(affix));
    }
    return pushdown::call(*name == "starts_with" ? Operation::starts_with
                                                 : Operation::ends_with,
                          std::move(subject), std::move(affix));
  }
  return None{};
}

auto translate_predicate(ast::expression const& expr,
                         ColumnModel const& columns, bool positive)
  -> Option<Expr>;

/// Translates a boolean combinator or comparison. Both `and` and `or` keep the
/// keep-or-drop outcome of a row when an operand turns from `null` to `false`,
/// so they pass `positive` on; `not` does not.
auto translate_binary(ast::binary_expr const& expr, ColumnModel const& columns,
                      bool positive) -> Option<Expr> {
  switch (expr.op) {
    case ast::binary_op::and_:
    case ast::binary_op::or_: {
      auto left = translate_predicate(expr.left, columns, positive);
      if (not left) {
        return None{};
      }
      auto right = translate_predicate(expr.right, columns, positive);
      if (not right) {
        return None{};
      }
      if (expr.op == ast::binary_op::and_) {
        return conjunction(std::move(*left), std::move(*right));
      }
      return disjunction(std::move(*left), std::move(*right));
    }
    case ast::binary_op::eq:
    case ast::binary_op::neq:
    case ast::binary_op::lt:
    case ast::binary_op::leq:
    case ast::binary_op::gt:
    case ast::binary_op::geq:
      return translate_comparison(expr, columns, positive);
    case ast::binary_op::in:
      return translate_in(expr, columns);
    default:
      return None{};
  }
}

/// Translates a bare boolean column used as predicate.
auto translate_boolean_column(ast::expression const& expr,
                              ColumnModel const& columns) -> Option<Expr> {
  auto column = resolve_column(expr, columns);
  if (not column or not is<BoolType>(column->type)) {
    return None{};
  }
  return std::move(column->expr);
}

auto translate_predicate(ast::expression const& expr,
                         ColumnModel const& columns, bool positive)
  -> Option<Expr> {
  return match(
    expr,
    [&](ast::binary_expr const& x) -> Option<Expr> {
      return translate_binary(x, columns, positive);
    },
    [&](ast::unary_expr const& x) -> Option<Expr> {
      if (x.op != ast::unary_op::not_) {
        return None{};
      }
      auto inner = translate_predicate(x.expr, columns, false);
      if (not inner) {
        return None{};
      }
      return Not{std::move(*inner)};
    },
    [&](ast::function_call const& x) -> Option<Expr> {
      return translate_predicate_function(x, columns);
    },
    [&](ast::root_field const&) -> Option<Expr> {
      return translate_boolean_column(expr, columns);
    },
    [&](ast::field_access const&) -> Option<Expr> {
      return translate_boolean_column(expr, columns);
    },
    [](auto const&) -> Option<Expr> {
      return None{};
    });
}

// -- Column adaptation --------------------------------------------------------

/// Returns whether `expr` names a string column.
auto is_string_column(ast::expression const& expr, ColumnModel const& columns)
  -> bool {
  auto column = resolve_column(expr, columns);
  return column and is<StringType>(column->type);
}

/// Returns the address if `expr` is an `ip` literal.
auto as_ip_literal(ast::expression const& expr) -> Option<ip> {
  auto literal = as_literal(expr);
  if (not literal) {
    return None{};
  }
  if (auto const* x = try_as<ip>(literal->value)) {
    return *x;
  }
  return None{};
}

/// Replaces `expr` with the canonical text of `x` as a string literal.
auto replace_with_text(ast::expression& expr, ip const& x) -> void {
  auto location = expr.get_location();
  expr = ast::expression{ast::constant{fmt::to_string(x), location}};
}

/// Wraps `expr` into a call of the `ip` function, resolved the way TQL resolves
/// it, or returns `false` if no such function is registered.
auto wrap_in_ip_call(ast::expression& expr) -> bool {
  auto reg = global_registry();
  if (not reg) {
    return false;
  }
  for (auto pkg : {entity_pkg_cfg, entity_pkg_std}) {
    auto path = entity_path{std::string{pkg}, {"ip"}, entity_ns::fn};
    if (not is<entity_ref>(reg->try_get(path))) {
      continue;
    }
    auto location = expr.get_location();
    auto fn = ast::entity{{ast::identifier{"ip", location}}};
    fn.ref = std::move(path);
    auto args = std::vector<ast::expression>{};
    args.push_back(std::move(expr));
    expr = ast::expression{
      ast::function_call{std::move(fn), std::move(args), location, true}};
    return true;
  }
  return false;
}

/// Adapts an equality between a string column and an `ip` literal.
auto adapt_equality(ast::binary_expr& expr, ColumnModel const& columns)
  -> void {
  if (is_string_column(expr.left, columns)) {
    if (auto x = as_ip_literal(expr.right)) {
      replace_with_text(expr.right, *x);
    }
  } else if (is_string_column(expr.right, columns)) {
    if (auto x = as_ip_literal(expr.left)) {
      replace_with_text(expr.left, *x);
    }
  }
}

/// Adapts a membership test of a string column in a list of `ip` literals or
/// in a subnet.
auto adapt_membership(ast::binary_expr& expr, ColumnModel const& columns)
  -> void {
  if (not is_string_column(expr.left, columns)) {
    return;
  }
  if (auto* list = try_as<ast::list>(expr.right)) {
    // TQL fixes the list's element type from its first element, so only a
    // list of addresses has a textual counterpart.
    auto addresses = std::vector<ip>{};
    for (auto const& item : list->items) {
      auto const* item_expr = try_as<ast::expression>(item);
      auto x = item_expr ? as_ip_literal(*item_expr) : None{};
      if (not x) {
        return;
      }
      addresses.push_back(*x);
    }
    for (auto i = size_t{0}; i < addresses.size(); ++i) {
      replace_with_text(as<ast::expression>(list->items[i]), addresses[i]);
    }
    return;
  }
  auto literal = as_literal(expr.right);
  if (literal and is<subnet>(literal->value)) {
    wrap_in_ip_call(expr.left);
  }
}

// -- Prefilters ---------------------------------------------------------------

/// Resolves `ip(column)` on a string column to `parse_ip`. The result is
/// nullable even for a non-nullable column, since the parse may fail.
auto resolve_parsed_ip(ast::expression const& expr, ColumnModel const& columns)
  -> Option<Scalar> {
  auto const* call = try_as<ast::function_call>(expr);
  if (not call or call->args.size() != 1) {
    return None{};
  }
  auto name = function_name(*call);
  if (not name or *name != "ip") {
    return None{};
  }
  auto column = resolve_column(call->args.front(), columns);
  if (not column or not is<StringType>(column->type)) {
    return None{};
  }
  return Scalar{.expr
                = pushdown::call(Operation::parse_ip, std::move(column->expr)),
                .type = IpType{.v4 = false},
                .nullable = true};
}

/// Keeps the rows that the target cannot parse, so that only the strings both
/// parsers accept need to agree.
auto keep_unparsed(Scalar const& parsed, Expr condition) -> Expr {
  return disjunction(is_null(parsed.expr), std::move(condition));
}

/// Translates a prefilter for a comparison that involves `ip(column)`.
///
/// Where TQL's `ip` yields a value, it is `null` for a string that does not
/// parse, and TQL's `!=` is `true` for `null`. A row that the target parses
/// but TQL does not would then be kept by TQL and dropped by the prefilter,
/// so `!=` gets none. Every other comparison is `true` only where TQL parsed
/// the string, and the target then either agrees or did not parse it.
auto translate_parsed_ip_comparison(ast::binary_expr const& expr,
                                    ColumnModel const& columns)
  -> Option<Expr> {
  if (expr.op == ast::binary_op::neq) {
    return None{};
  }
  auto op = expr.op;
  auto parsed = resolve_parsed_ip(expr.left, columns);
  auto literal = as_ip_literal(expr.right);
  if (not parsed or not literal) {
    parsed = resolve_parsed_ip(expr.right, columns);
    literal = as_ip_literal(expr.left);
    if (not parsed or not literal) {
      return None{};
    }
    op = flip_comparison(op);
  }
  return keep_unparsed(*parsed, translate_ip_comparison(parsed->expr, false,
                                                        IpType{.v4 = false}, op,
                                                        *literal));
}

/// Translates a prefilter for `ip(column) in list` or `ip(column) in subnet`.
auto translate_parsed_ip_membership(ast::binary_expr const& expr,
                                    ColumnModel const& columns)
  -> Option<Expr> {
  auto parsed = resolve_parsed_ip(expr.left, columns);
  if (not parsed) {
    return None{};
  }
  if (auto const* list = try_as<ast::list>(expr.right)) {
    auto plain = Scalar{parsed->expr, parsed->type, false};
    auto set = translate_in_list(plain, *list);
    if (not set) {
      return None{};
    }
    return keep_unparsed(*parsed, std::move(*set));
  }
  auto literal = as_literal(expr.right);
  auto const* sn = literal ? try_as<subnet>(literal->value) : nullptr;
  if (not sn) {
    return None{};
  }
  return keep_unparsed(
    *parsed, translate_ip_in_subnet(parsed->expr, IpType{.v4 = false}, *sn));
}

/// Appends the conjuncts of `expr` to `out`, splitting nested `and`s.
///
/// Splitting lets a translatable conjunct be pushed while an earlier one stays
/// local, so the local one then sees only the rows that survived the pushed
/// one. This is safe because a conjunction selects the same rows in any order
/// and `where` predicates are pure. It only changes how often a local
/// predicate is evaluated, and with it the number of warnings it emits for
/// rows that the result never contains. TQL's `and` already evaluates its
/// right side only on rows the left side did not decide, and `export` splits a
/// conjunction the same way.
auto collect_conjuncts(ast::expression expr, ir::OptimizeFilter& out) -> void {
  if (auto* binary = try_as<ast::binary_expr>(expr);
      binary and binary->op == ast::binary_op::and_) {
    collect_conjuncts(std::move(binary->left), out);
    collect_conjuncts(std::move(binary->right), out);
    return;
  }
  out.push_back(std::move(expr));
}

} // namespace

// -- Entry points -------------------------------------------------------------

auto adapt_to_columns(ast::expression& expr, ColumnModel const& columns)
  -> void {
  match(
    expr,
    [&](ast::binary_expr& x) {
      switch (x.op) {
        case ast::binary_op::and_:
        case ast::binary_op::or_:
          adapt_to_columns(x.left, columns);
          adapt_to_columns(x.right, columns);
          return;
        case ast::binary_op::eq:
        case ast::binary_op::neq:
          adapt_equality(x, columns);
          return;
        case ast::binary_op::in:
          adapt_membership(x, columns);
          return;
        default:
          return;
      }
    },
    [&](ast::unary_expr& x) {
      if (x.op == ast::unary_op::not_) {
        adapt_to_columns(x.expr, columns);
      }
    },
    [](auto&) {});
}

auto translate_predicate(ast::expression const& expr,
                         ColumnModel const& columns) -> Option<Expr> {
  // A predicate at the top of the filter chain drops a row on `false` and on
  // `null` alike.
  return translate_predicate(expr, columns, true);
}

auto translate_prefilter(ast::expression const& expr,
                         ColumnModel const& columns) -> Option<Expr> {
  // An exact translation is the tightest prefilter. It may use the positive
  // form, since a prefilter drops rows on `false` and `null` alike.
  if (auto exact = translate_predicate(expr, columns, true)) {
    return exact;
  }
  auto const* binary = try_as<ast::binary_expr>(expr);
  if (not binary) {
    return None{};
  }
  switch (binary->op) {
    case ast::binary_op::and_: {
      // A conjunction is `true` only where every conjunct is, so any subset
      // of their prefilters is a prefilter.
      auto left = translate_prefilter(binary->left, columns);
      auto right = translate_prefilter(binary->right, columns);
      if (left and right) {
        return conjunction(std::move(*left), std::move(*right));
      }
      return left ? std::move(left) : std::move(right);
    }
    case ast::binary_op::or_: {
      auto left = translate_prefilter(binary->left, columns);
      if (not left) {
        return None{};
      }
      auto right = translate_prefilter(binary->right, columns);
      if (not right) {
        return None{};
      }
      return disjunction(std::move(*left), std::move(*right));
    }
    case ast::binary_op::eq:
    case ast::binary_op::neq:
    case ast::binary_op::lt:
    case ast::binary_op::leq:
    case ast::binary_op::gt:
    case ast::binary_op::geq:
      return translate_parsed_ip_comparison(*binary, columns);
    case ast::binary_op::in:
      return translate_parsed_ip_membership(*binary, columns);
    default:
      return None{};
  }
}

auto split_filter(ir::OptimizeFilter filter, ColumnModel const& columns,
                  Renderer const& renderer) -> FilterSplit {
  auto conjuncts = ir::OptimizeFilter{};
  for (auto& expr : filter) {
    adapt_to_columns(expr, columns);
    collect_conjuncts(std::move(expr), conjuncts);
  }
  // A translation is pushed only if the renderer can spell it.
  auto render = [&](Option<Expr> const& expr) -> Option<std::string> {
    if (not expr) {
      return None{};
    }
    return renderer.render(*expr);
  };
  auto result = FilterSplit{};
  for (auto& expr : conjuncts) {
    if (auto pushed = render(translate_predicate(expr, columns))) {
      result.pushed.push_back(std::move(*pushed));
      continue;
    }
    if (auto pushed = render(translate_prefilter(expr, columns))) {
      result.pushed.push_back(std::move(*pushed));
    }
    result.remaining.push_back(std::move(expr));
  }
  return result;
}

} // namespace tenzir::pushdown
