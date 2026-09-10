//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/sql_pushdown.hpp"

#include "clickhouse/arguments.hpp"
#include "clickhouse/block_to_table_slice.hpp"
#include "clickhouse/transformers.hpp"
#include "tenzir/checked_math.hpp"
#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/detail/escapers.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/subnet.hpp"
#include "tenzir/tql2/entity_path.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/uuid.hpp"
#include "tenzir/variant_traits.hpp"

#include <clickhouse/columns/factory.h>
#include <clickhouse/types/types.h>
#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <concepts>
#include <limits>
#include <ranges>

namespace tenzir::plugins::clickhouse {

namespace {

using namespace std::string_view_literals;

// -- ClickHouse type parsing --------------------------------------------------

/// Strips `Nullable(...)` and `LowCardinality(...)` wrappers. Sets `nullable`
/// if a `Nullable` wrapper was present.
auto unwrap_column_type(std::string_view type, bool& nullable)
  -> std::string_view {
  while (true) {
    if (auto inner = unwrap_clickhouse_type_call(type, "Nullable")) {
      nullable = true;
      type = *inner;
      continue;
    }
    if (auto inner = unwrap_clickhouse_type_call(type, "LowCardinality")) {
      type = *inner;
      continue;
    }
    return type;
  }
}

/// Reads the element names of an `Enum` type from the type object that the
/// decoder builds, so that they are exactly what TQL sees. The client's type
/// parser takes a quoted name verbatim, escapes included: a label rendered as
/// `'a\\b'` reaches TQL as the four characters `a\\b`. Re-quoting such a
/// name therefore reproduces the server's own rendering, which makes it the
/// SQL literal for the label. The numeric values do not matter.
auto parse_enum(std::string_view type) -> Option<SqlType> {
  auto column = ::clickhouse::CreateColumnByType(std::string{type});
  auto const* enum_type
    = column ? column->Type()->As<::clickhouse::EnumType>() : nullptr;
  if (not enum_type) {
    return None{};
  }
  auto result = SqlEnum{};
  for (auto it = enum_type->BeginValueToName();
       it != enum_type->EndValueToName(); ++it) {
    result.names.emplace(it->second);
  }
  return result;
}

/// Maps a bare ClickHouse type to the description of how TQL sees its values.
/// Types that TQL decodes ambiguously (e.g. `Decimal` scaling) or not at all
/// are left out.
auto parse_type(std::string_view type) -> Option<SqlType> {
  constexpr auto integers = std::array<std::pair<std::string_view, SqlInt>, 8>{{
    {"Int8", {.bits = 8, .is_signed = true}},
    {"Int16", {.bits = 16, .is_signed = true}},
    {"Int32", {.bits = 32, .is_signed = true}},
    {"Int64", {.bits = 64, .is_signed = true}},
    {"UInt8", {.bits = 8, .is_signed = false}},
    {"UInt16", {.bits = 16, .is_signed = false}},
    {"UInt32", {.bits = 32, .is_signed = false}},
    {"UInt64", {.bits = 64, .is_signed = false}},
  }};
  if (type == "Bool") {
    return SqlBool{};
  }
  if (type == "String") {
    return SqlString{};
  }
  for (auto const& [name, info] : integers) {
    if (type == name) {
      return info;
    }
  }
  if (type == "Float32" or type == "Float64") {
    return SqlFloat{};
  }
  if (type == "UUID") {
    return SqlUuid{};
  }
  if (type == "IPv4") {
    return SqlIp{.v4 = true};
  }
  if (type == "IPv6") {
    return SqlIp{.v4 = false};
  }
  if (type == "Date") {
    return SqlTime{.family = SqlTime::Family::date};
  }
  if (type == "Date32") {
    return SqlTime{.family = SqlTime::Family::date32};
  }
  if (auto args = unwrap_clickhouse_type_call(type, "DateTime64")) {
    auto list = split_top_level_clickhouse_type_arguments(*args);
    auto precision = size_t{};
    if (list.empty() or not parse_clickhouse_size(list.front(), precision)
        or precision > 9) {
      return None{};
    }
    return SqlTime{.family = SqlTime::Family::datetime64,
                   .precision = static_cast<uint8_t>(precision)};
  }
  // The time zone argument only affects how ClickHouse renders the value.
  if (type == "DateTime" or unwrap_clickhouse_type_call(type, "DateTime")) {
    return SqlTime{.family = SqlTime::Family::datetime};
  }
  if (auto args = unwrap_clickhouse_type_call(type, "FixedString")) {
    auto length = size_t{};
    if (not parse_clickhouse_size(*args, length)) {
      return None{};
    }
    return SqlFixedString{.length = length};
  }
  for (auto name : {"Enum8"sv, "Enum16"sv, "Enum"sv}) {
    if (unwrap_clickhouse_type_call(type, name)) {
      return parse_enum(type);
    }
  }
  return None{};
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
/// result in TQL and ClickHouse.
///
/// Same-kind comparisons always do. Mixed comparisons differ in how they are
/// evaluated: TQL converts the integer side to `double` and compares doubles,
/// while ClickHouse compares the two numbers exactly. When the integer side is
/// the literal, both agree as long as the literal converts exactly. When the
/// integer side is the column, its values are what gets rounded, and both
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

/// Renders a boolean, numeric, or string constant as a SQL literal.
auto render_constant(ast::constant const& constant) -> std::string {
  return match(
    constant.value,
    [](bool x) -> std::string {
      return x ? "true" : "false";
    },
    [](int64_t x) -> std::string {
      return fmt::to_string(x);
    },
    [](uint64_t x) -> std::string {
      return fmt::to_string(x);
    },
    [](double x) -> std::string {
      // `{}` picks the shortest representation that round-trips, but renders
      // an integral value without a fraction. ClickHouse would read that as an
      // integer and, in arithmetic with an integer column, compute exactly
      // where TQL computes in `double`. A trailing `.` keeps it a `Float64`.
      auto rendered = fmt::format("{}", x);
      if (rendered.find_first_of(".e") == std::string::npos) {
        rendered += '.';
      }
      return rendered;
    },
    [](std::string const& x) -> std::string {
      return quote_sql_string(x);
    },
    [](auto const&) -> std::string {
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

auto render_comparison(ast::binary_op op) -> std::string_view {
  switch (op) {
    case ast::binary_op::eq:
      return "=";
    case ast::binary_op::neq:
      return "!=";
    case ast::binary_op::lt:
      return "<";
    case ast::binary_op::leq:
      return "<=";
    case ast::binary_op::gt:
      return ">";
    case ast::binary_op::geq:
      return ">=";
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

/// Renders `sql == literal` or `sql != literal` with TQL's null semantics,
/// where `null` never equals a value.
auto render_equality(std::string const& sql, bool nullable, ast::binary_op op,
                     std::string const& literal) -> std::string {
  if (op == ast::binary_op::eq) {
    if (nullable) {
      return fmt::format("({0} IS NOT NULL AND {0} = {1})", sql, literal);
    }
    return fmt::format("{} = {}", sql, literal);
  }
  if (nullable) {
    return fmt::format("({0} IS NULL OR {0} != {1})", sql, literal);
  }
  return fmt::format("{} != {}", sql, literal);
}

/// The outcome of an equality whose literal can never match, such as an
/// unknown enum name. TQL yields `false` for every row, `null` included, and
/// `!=` yields `true`.
auto render_never_equal(ast::binary_op op) -> std::string {
  return op == ast::binary_op::eq ? "false" : "true";
}

// -- Time ---------------------------------------------------------------------

constexpr auto nanos_per_second = int64_t{1'000'000'000};
constexpr auto nanos_per_day = nanos_per_second * 86'400;

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

auto format_date(int64_t days) -> std::string {
  auto ymd = std::chrono::year_month_day{
    std::chrono::sys_days{std::chrono::days{days}}};
  return fmt::format("{:04}-{:02}-{:02}", int{ymd.year()},
                     unsigned{ymd.month()}, unsigned{ymd.day()});
}

/// The tick grid of a temporal column.
///
/// `from_clickhouse` decodes every temporal type to nanoseconds since the
/// epoch in an `int64`, and TQL compares those. A stored value is one tick of
/// `unit` nanoseconds. Ticks outside `[lo, hi]` either do not exist in the
/// column's type or overflow the conversion, in which case TQL sees `null`.
struct TimeLayout {
  SqlTime type;
  int64_t unit;
  int64_t lo;
  int64_t hi;
  /// Whether the type holds ticks below `lo` or above `hi` that TQL decodes
  /// to `null`. Such rows need a guard where the comparison would otherwise
  /// see the stored value.
  bool guard_lo;
  bool guard_hi;

  static auto of(SqlTime type) -> TimeLayout {
    constexpr auto max = std::numeric_limits<int64_t>::max();
    switch (type.family) {
      case SqlTime::Family::date:
        // `UInt16` days, 1970-01-01 to 2149-06-06, all decodable.
        return {type, nanos_per_day, 0, 65535, false, false};
      case SqlTime::Family::date32:
        // `Int32` days, 1900-01-01 to 2299-12-31. Days past 2262-04-11 do not
        // fit into nanoseconds. ClickHouse saturates literals before 1900 and
        // none of its functions produce such days, so no lower guard.
        return {type, nanos_per_day, -25567, max / nanos_per_day, false, true};
      case SqlTime::Family::datetime:
        // `UInt32` seconds, all decodable.
        return {type, nanos_per_second, 0, 4294967295, false, false};
      case SqlTime::Family::datetime64: {
        // `Int64` ticks of `10^-precision` seconds. Only at nanosecond
        // precision does every tick fit; coarser precisions reach past 2262.
        auto unit = checked_pow(int64_t{10}, 9 - type.precision);
        TENZIR_ASSERT(unit);
        auto hi = max / *unit;
        // `-2^63` is not a multiple of ten, so its floor and `-hi` coincide.
        auto lo = *unit == 1 ? std::numeric_limits<int64_t>::min() : -hi;
        auto guarded = type.precision < 9;
        return {type, *unit, lo, hi, guarded, guarded};
      }
    }
    TENZIR_UNREACHABLE();
  }

  /// Renders `ticks` as a literal of exactly the column's type. Requires
  /// `ticks` to lie within `[lo, hi]`.
  ///
  /// Every form avoids the session time zone: date strings and the
  /// `fromUnixTimestamp64*` family denote an instant regardless of it, and an
  /// explicit `'UTC'` fixes the `DateTime` parse. `toDateTime64` from a string
  /// would be the natural choice for `DateTime64`, but it saturates before
  /// 1900, so the literal is built from its integer tick count instead.
  auto literal(int64_t ticks) const -> std::string {
    TENZIR_ASSERT(ticks >= lo and ticks <= hi);
    switch (type.family) {
      case SqlTime::Family::date:
        return fmt::format("toDate('{}')", format_date(ticks));
      case SqlTime::Family::date32:
        return fmt::format("toDate32('{}')", format_date(ticks));
      case SqlTime::Family::datetime: {
        auto [days, seconds] = floor_div(ticks, 86'400);
        return fmt::format("toDateTime('{} {:02}:{:02}:{:02}', 'UTC')",
                           format_date(days), seconds / 3600, seconds / 60 % 60,
                           seconds % 60);
      }
      case SqlTime::Family::datetime64:
        switch (type.precision) {
          case 3:
            return fmt::format("fromUnixTimestamp64Milli({})", ticks);
          case 6:
            return fmt::format("fromUnixTimestamp64Micro({})", ticks);
          case 9:
            return fmt::format("fromUnixTimestamp64Nano({})", ticks);
          default:
            // The tick is within `[lo, hi]`, so its nanoseconds fit.
            return fmt::format("CAST(fromUnixTimestamp64Nano({}), "
                               "'DateTime64({})')",
                               ticks * unit, type.precision);
        }
    }
    TENZIR_UNREACHABLE();
  }

  /// Renders the condition under which TQL sees a value rather than `null`
  /// for the column, or `None` if it always does.
  auto guard(std::string const& sql) const -> Option<std::string> {
    auto parts = std::vector<std::string>{};
    if (guard_lo) {
      parts.push_back(fmt::format("{} >= {}", sql, literal(lo)));
    }
    if (guard_hi) {
      parts.push_back(fmt::format("{} <= {}", sql, literal(hi)));
    }
    if (parts.empty()) {
      return None{};
    }
    return fmt::format("{}", fmt::join(parts, " AND "));
  }

  /// Renders the column as TQL sees it: `NULL` where the stored tick does not
  /// decode.
  auto view(std::string const& sql) const -> std::string {
    if (auto condition = guard(sql)) {
      return fmt::format("if({}, {}, NULL)", *condition, sql);
    }
    return sql;
  }

  /// Returns the tick of `t` if `t` lies on the grid and within range.
  auto exact_tick(time t) const -> Option<int64_t> {
    auto [ticks, remainder] = floor_div(t.time_since_epoch().count(), unit);
    if (remainder != 0 or ticks < lo or ticks > hi) {
      return None{};
    }
    return ticks;
  }
};

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
/// comparison with `AND`, which leaves it analyzable for the primary key.
/// Elsewhere the comparison yields `NULL` outright.
auto translate_time_comparison(std::string const& sql, bool nullable,
                               SqlTime type, ast::binary_op op, time literal,
                               bool positive) -> std::string {
  auto layout = TimeLayout::of(type);
  if (is_equality(op)) {
    auto ticks = layout.exact_tick(literal);
    if (not ticks) {
      return render_never_equal(op);
    }
    return render_equality(sql, nullable, op, layout.literal(*ticks));
  }
  auto [ticks, remainder]
    = floor_div(literal.time_since_epoch().count(), layout.unit);
  if (remainder != 0) {
    // With `t` strictly between the ticks `g` and `g + 1`, `x < t` holds
    // exactly when `x <= g`, and `x >= t` exactly when `x > g`.
    if (op == ast::binary_op::lt) {
      op = ast::binary_op::leq;
    } else if (op == ast::binary_op::geq) {
      op = ast::binary_op::gt;
    }
  }
  if (ticks < layout.lo) {
    // Every stored value is greater than the literal.
    op = is_less(op) ? ast::binary_op::lt : ast::binary_op::geq;
    ticks = layout.lo;
  } else if (ticks > layout.hi) {
    // Every stored value is less than the literal.
    op = is_less(op) ? ast::binary_op::leq : ast::binary_op::gt;
    ticks = layout.hi;
  }
  auto comparison = fmt::format("{} {} {}", sql, render_comparison(op),
                                layout.literal(ticks));
  auto guard = layout.guard(sql);
  if (not guard) {
    return comparison;
  }
  if (positive) {
    return fmt::format("({} AND {})", *guard, comparison);
  }
  return fmt::format("if({}, {}, NULL)", *guard, comparison);
}

// -- IP addresses -------------------------------------------------------------

/// The IPv4-mapped range that an `IPv4` column covers in TQL's 16-byte
/// address space.
auto v4_range() -> std::pair<ip, ip> {
  return {ip::v4(uint32_t{0}), ip::v4(uint32_t{0xffffffff})};
}

/// Renders `x` as a literal of the column's type. TQL prints IPv4-mapped
/// addresses in dotted form, which `toIPv6` maps back to the same 16 bytes.
auto ip_literal(ip const& x, bool v4) -> std::string {
  return fmt::format("{}({})", v4 ? "toIPv4" : "toIPv6",
                     quote_sql_string(fmt::to_string(x)));
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
/// TQL compares the 16-byte forms, with IPv4 addresses mapped into IPv6.
/// ClickHouse orders both `IPv4` and `IPv6` the same way, so a literal of the
/// column's family translates directly. An IPv6 literal against an `IPv4`
/// column lies entirely below or above the mapped range, which fixes the
/// outcome; the ordering forms clamp to the range's edge to stay `null` for
/// `null` rows.
auto translate_ip_comparison(std::string const& sql, bool nullable, SqlIp type,
                             ast::binary_op op, ip const& literal)
  -> std::string {
  if (type.v4 and not literal.is_v4()) {
    if (is_equality(op)) {
      return render_never_equal(op);
    }
    auto [lo, hi] = v4_range();
    auto below = literal < lo;
    op = below ? (is_less(op) ? ast::binary_op::lt : ast::binary_op::geq)
               : (is_less(op) ? ast::binary_op::leq : ast::binary_op::gt);
    return fmt::format("{} {} {}", sql, render_comparison(op),
                       ip_literal(below ? lo : hi, true));
  }
  auto rendered = ip_literal(literal, type.v4);
  if (is_equality(op)) {
    return render_equality(sql, nullable, op, rendered);
  }
  return fmt::format("{} {} {}", sql, render_comparison(op), rendered);
}

/// Translates `column in subnet` into a range check. Both TQL and SQL yield
/// `null` for a `null` address, so no guard is needed, and a range that
/// misses an `IPv4` column entirely becomes a comparison that is `false` for
/// every address and `null` for none.
auto translate_ip_in_subnet(std::string const& sql, SqlIp type,
                            subnet const& sn) -> std::string {
  auto lo = sn.network();
  auto hi = subnet_end(sn);
  if (type.v4) {
    auto [v4_lo, v4_hi] = v4_range();
    if (hi < v4_lo or lo > v4_hi) {
      return fmt::format("{} < {}", sql, ip_literal(v4_lo, true));
    }
    lo = std::max(lo, v4_lo);
    hi = std::min(hi, v4_hi);
  }
  return fmt::format("{} BETWEEN {} AND {}", sql, ip_literal(lo, type.v4),
                     ip_literal(hi, type.v4));
}

// -- Text-like types ----------------------------------------------------------

/// Returns the literal that compares equal to `text` in a `FixedString(N)`
/// column, or `None` if no stored value can. TQL sees all `N` stored bytes,
/// zero padding included, so only a literal of exactly `N` bytes can equal
/// one. ClickHouse pads a shorter literal for the comparison, which would
/// match where TQL does not.
auto fixed_string_literal(std::string const& text, SqlFixedString const& type)
  -> Option<std::string> {
  if (text.size() != type.length) {
    return None{};
  }
  return quote_sql_string(text);
}

/// Returns the literal for an enum name, or `None` if the enum has no such
/// element. ClickHouse would reject or never match an unknown name, depending
/// on its version; TQL never matches it. A known name is the server's own
/// rendering of the label between quotes (see `parse_enum`), so quoting it
/// verbatim yields the literal that ClickHouse parses back to the label.
auto enum_literal(std::string const& text, SqlEnum const& type)
  -> Option<std::string> {
  if (not type.names.contains(text)) {
    return None{};
  }
  return fmt::format("'{}'", text);
}

/// Returns the literal for a UUID, or `None` if `text` is not in the canonical
/// lowercase form that TQL sees. ClickHouse parses uppercase digits too, so an
/// uppercase literal would match where TQL does not; parsing and printing the
/// text back tells the two apart.
auto uuid_literal(std::string const& text) -> Option<std::string> {
  auto parsed = uuid{};
  if (not parsers::uuid(text, parsed) or fmt::to_string(parsed) != text) {
    return None{};
  }
  return quote_sql_string(text);
}

// -- Scalars ------------------------------------------------------------------

/// A translated value expression: a column, or arithmetic on one.
struct SqlScalar {
  std::string sql;
  SqlType type;
  bool nullable;
};

/// Renders `scalar` as TQL sees it: `NULL` where a temporal value does not
/// decode.
auto tql_view(SqlScalar const& scalar) -> std::string {
  if (auto const* type = try_as<SqlTime>(scalar.type)) {
    return TimeLayout::of(*type).view(scalar.sql);
  }
  return scalar.sql;
}

/// Resolves a field path expression to a column of `schema`.
auto resolve_column(ast::expression const& expr, SqlSchema const& schema)
  -> Option<SqlScalar> {
  auto path = ast::field_path::try_from(expr);
  if (not path or path->path().empty()) {
    return None{};
  }
  auto segments = std::vector<std::string>{};
  auto sql = std::string{};
  for (auto const& segment : path->path()) {
    segments.push_back(segment.id.name);
    if (not sql.empty()) {
      sql += '.';
    }
    sql += quote_sql_identifier(segment.id.name);
  }
  auto column = schema.find(segments);
  if (not column) {
    return None{};
  }
  return SqlScalar{.sql = std::move(sql),
                   .type = std::move(column->type),
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

auto render_arithmetic(ast::binary_op op) -> std::string_view {
  switch (op) {
    case ast::binary_op::add:
      return "+";
    case ast::binary_op::sub:
      return "-";
    case ast::binary_op::mul:
      return "*";
    case ast::binary_op::div:
      return "/";
    default:
      TENZIR_UNREACHABLE();
  }
}

/// Translates arithmetic between a numeric column and a literal.
///
/// Both sides compute in `double` when either operand is one, converting an
/// integer operand with the same IEEE 754 rounding, so floating-point
/// arithmetic translates as long as no division by zero is involved: TQL
/// yields `null` for it, ClickHouse an infinity. Division always computes in
/// `double`. Integer arithmetic differs on overflow, where TQL yields `null`
/// and ClickHouse wraps around. It is only pushed when overflow is impossible:
/// on columns of at most 32 bits, with a literal below 2^31, so that the
/// result fits comfortably into 64 bits on both sides. Unsigned columns yield
/// an unsigned result in TQL, so only literals that cannot take the result
/// below zero are allowed.
auto translate_arithmetic(ast::binary_expr const& expr, SqlSchema const& schema)
  -> Option<SqlScalar> {
  auto op = expr.op;
  auto column = resolve_column(expr.left, schema);
  auto literal = as_literal(expr.right);
  auto column_left = true;
  if (not column or not literal) {
    column = resolve_column(expr.right, schema);
    literal = as_literal(expr.left);
    column_left = false;
    // `literal / column` divides by zero wherever the column is zero.
    if (not column or not literal or op == ast::binary_op::div) {
      return None{};
    }
  }
  auto const* integer = try_as<SqlInt>(column->type);
  if (not integer and not is<SqlFloat>(column->type)) {
    return None{};
  }
  auto result_type = Option<SqlType>{};
  match(
    literal->value,
    [&](double x) {
      if (std::isfinite(x) and (op != ast::binary_op::div or x != 0.0)) {
        result_type = SqlFloat{};
      }
    },
    [&](int64_t x) {
      if (op == ast::binary_op::div) {
        if (x != 0) {
          result_type = SqlFloat{};
        }
        return;
      }
      if (not integer) {
        result_type = SqlFloat{};
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
          result_type = SqlInt{.bits = 64, .is_signed = true};
          return;
        }
      }
      result_type = SqlInt{.bits = 64, .is_signed = integer->is_signed};
    },
    [&](uint64_t) {
      // Only literals beyond the `int64` range are `uint64`: too large for
      // integer arithmetic, but a `double` operand like any other otherwise.
      if (not integer or op == ast::binary_op::div) {
        result_type = SqlFloat{};
      }
    },
    [](auto const&) {});
  if (not result_type) {
    return None{};
  }
  auto rendered = render_constant(*literal);
  auto sql = column_left ? fmt::format("({} {} {})", column->sql,
                                       render_arithmetic(op), rendered)
                         : fmt::format("({} {} {})", rendered,
                                       render_arithmetic(op), column->sql);
  return SqlScalar{.sql = std::move(sql),
                   .type = std::move(*result_type),
                   .nullable = column->nullable};
}

/// Translates a function call that yields a scalar.
auto translate_scalar_function(ast::function_call const& call,
                               SqlSchema const& schema) -> Option<SqlScalar> {
  auto name = function_name(call);
  if (not name or call.args.size() != 1) {
    return None{};
  }
  if (*name == "length_bytes") {
    // Both count bytes, including the zero padding of a `FixedString`.
    auto column = resolve_column(call.args.front(), schema);
    if (not column
        or not(is<SqlString>(column->type)
               or is<SqlFixedString>(column->type))) {
      return None{};
    }
    return SqlScalar{.sql = fmt::format("length({})", column->sql),
                     .type = SqlInt{.bits = 64, .is_signed = false},
                     .nullable = column->nullable};
  }
  return None{};
}

auto translate_scalar(ast::expression const& expr, SqlSchema const& schema)
  -> Option<SqlScalar> {
  if (auto column = resolve_column(expr, schema)) {
    return column;
  }
  return match(
    expr,
    [&](ast::binary_expr const& x) -> Option<SqlScalar> {
      switch (x.op) {
        case ast::binary_op::add:
        case ast::binary_op::sub:
        case ast::binary_op::mul:
        case ast::binary_op::div:
          return translate_arithmetic(x, schema);
        default:
          return None{};
      }
    },
    [&](ast::function_call const& x) -> Option<SqlScalar> {
      return translate_scalar_function(x, schema);
    },
    [](auto const&) -> Option<SqlScalar> {
      return None{};
    });
}

// -- Comparisons --------------------------------------------------------------

/// Translates a comparison of a scalar with a literal that is not `null`.
/// `positive` tells whether `false` and `null` are interchangeable in the
/// surrounding predicate.
auto translate_literal_comparison(SqlScalar const& scalar, ast::binary_op op,
                                  ast::constant const& literal, bool positive)
  -> Option<std::string> {
  auto const* text = try_as<std::string>(literal.value);
  // Renders an equality against the literal of a text-like column, or the
  // constant outcome if no stored value can match.
  auto text_equality = [&](auto make_literal) -> Option<std::string> {
    if (not text or not is_equality(op)) {
      return None{};
    }
    auto rendered = make_literal(*text);
    if (not rendered) {
      return render_never_equal(op);
    }
    return render_equality(scalar.sql, scalar.nullable, op, *rendered);
  };
  auto number = [&](bool integer) -> Option<std::string> {
    if (not numeric_literal_fits(integer, literal)) {
      return None{};
    }
    auto rendered = render_constant(literal);
    if (is_equality(op)) {
      return render_equality(scalar.sql, scalar.nullable, op, rendered);
    }
    // Ordering yields `null` for `null` operands in both TQL and SQL, so no
    // guard is needed. Both sides agree on the order, including IEEE 754
    // semantics for `nan`: neither TQL nor ClickHouse's comparison operators
    // order it.
    return fmt::format("{} {} {}", scalar.sql, render_comparison(op), rendered);
  };
  return match(
    scalar.type,
    [&](SqlBool const&) -> Option<std::string> {
      if (not is<bool>(literal.value) or not is_equality(op)) {
        return None{};
      }
      return render_equality(scalar.sql, scalar.nullable, op,
                             render_constant(literal));
    },
    [&](SqlInt const&) -> Option<std::string> {
      return number(true);
    },
    [&](SqlFloat const&) -> Option<std::string> {
      return number(false);
    },
    [&](SqlString const&) -> Option<std::string> {
      if (not text) {
        return None{};
      }
      auto rendered = quote_sql_string(*text);
      if (is_equality(op)) {
        return render_equality(scalar.sql, scalar.nullable, op, rendered);
      }
      // Both compare bytes as unsigned values, shorter prefix first.
      return fmt::format("{} {} {}", scalar.sql, render_comparison(op),
                         rendered);
    },
    [&](SqlFixedString const& type) -> Option<std::string> {
      return text_equality([&](std::string const& x) {
        return fixed_string_literal(x, type);
      });
    },
    [&](SqlEnum const& type) -> Option<std::string> {
      return text_equality([&](std::string const& x) {
        return enum_literal(x, type);
      });
    },
    [&](SqlUuid const&) -> Option<std::string> {
      return text_equality(uuid_literal);
    },
    [&](SqlTime const& type) -> Option<std::string> {
      auto const* t = try_as<time>(literal.value);
      if (not t) {
        return None{};
      }
      return translate_time_comparison(scalar.sql, scalar.nullable, type, op,
                                       *t, positive);
    },
    [&](SqlIp const& type) -> Option<std::string> {
      auto const* x = try_as<ip>(literal.value);
      if (not x) {
        return None{};
      }
      return translate_ip_comparison(scalar.sql, scalar.nullable, type, op, *x);
    });
}

/// Renders a comparison of two SQL values with TQL's null semantics. TQL
/// treats `null` as a value for `==`, `!=`, `<=`, and `>=`: two nulls are
/// equal, and thus also less-or-equal, while `<` and `>` yield `null` like
/// SQL does. A `null` against a value is unequal and unordered on both sides.
auto render_pairwise(std::string const& left, bool left_nullable,
                     std::string const& right, bool right_nullable,
                     ast::binary_op op) -> std::string {
  auto plain = fmt::format("{} {} {}", left, render_comparison(op), right);
  auto both_null = fmt::format("({} IS NULL AND {} IS NULL)", left, right);
  switch (op) {
    case ast::binary_op::eq:
    case ast::binary_op::neq: {
      if (not left_nullable and not right_nullable) {
        return plain;
      }
      auto equal = fmt::format("{} = {}", left, right);
      // Both sides `null` or both present and equal. Every part is a
      // definite boolean, so `NOT` on it stays definite.
      auto equality = left_nullable and right_nullable
                        ? fmt::format("({} OR ({} IS NOT NULL AND {} IS NOT "
                                      "NULL AND {}))",
                                      both_null, left, right, equal)
                        : fmt::format("({} IS NOT NULL AND {})",
                                      left_nullable ? left : right, equal);
      return op == ast::binary_op::eq ? equality
                                      : fmt::format("NOT {}", equality);
    }
    case ast::binary_op::leq:
    case ast::binary_op::geq:
      if (left_nullable and right_nullable) {
        return fmt::format("({} OR {})", both_null, plain);
      }
      return plain;
    default:
      return plain;
  }
}

/// Translates a comparison between two scalars.
///
/// Only same-kind pairs are pushed. TQL compares mixed integers and doubles
/// by converting the integer to `double`, which ClickHouse does not, and it
/// compares the textual form of enums, UUIDs, and fixed strings, where
/// ClickHouse pads or converts. Temporal columns must share their exact
/// storage type, since ClickHouse converts one side otherwise, and the
/// conversion depends on the session time zone or overflows.
auto translate_pairwise_comparison(ast::binary_expr const& expr,
                                   SqlSchema const& schema)
  -> Option<std::string> {
  auto left = translate_scalar(expr.left, schema);
  if (not left) {
    return None{};
  }
  auto right = translate_scalar(expr.right, schema);
  if (not right) {
    return None{};
  }
  auto op = expr.op;
  auto left_sql = left->sql;
  auto right_sql = right->sql;
  auto left_nullable = left->nullable;
  auto right_nullable = right->nullable;
  auto compatible = match(
    std::tie(left->type, right->type),
    [](SqlInt const&, SqlInt const&) {
      // ClickHouse compares signed and unsigned integers exactly, as does TQL.
      return true;
    },
    [](SqlFloat const&, SqlFloat const&) {
      return true;
    },
    [](SqlString const&, SqlString const&) {
      return true;
    },
    [](SqlIp const&, SqlIp const&) {
      // ClickHouse maps `IPv4` into `IPv6` for mixed comparisons, as TQL does.
      return true;
    },
    [&](SqlBool const&, SqlBool const&) {
      return is_equality(op);
    },
    [&](SqlUuid const&, SqlUuid const&) {
      return is_equality(op);
    },
    [](SqlFixedString const& x, SqlFixedString const& y) {
      return x.length == y.length;
    },
    [&](SqlTime const& x, SqlTime const& y) {
      if (x.family != y.family or x.precision != y.precision) {
        return false;
      }
      // Values that TQL decodes to `null` must compare as `null`.
      auto layout = TimeLayout::of(x);
      if (layout.guard_lo or layout.guard_hi) {
        left_sql = tql_view(*left);
        right_sql = tql_view(*right);
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
  return render_pairwise(left_sql, left_nullable, right_sql, right_nullable,
                         op);
}

auto translate_comparison(ast::binary_expr const& expr, SqlSchema const& schema,
                          bool positive) -> Option<std::string> {
  // Exactly one side must be a literal; otherwise both must be scalars.
  auto op = expr.op;
  auto const* scalar_expr = &expr.left;
  auto literal = as_literal(expr.right);
  if (not literal) {
    literal = as_literal(expr.left);
    if (not literal) {
      return translate_pairwise_comparison(expr, schema);
    }
    scalar_expr = &expr.right;
    op = flip_comparison(op);
  }
  auto scalar = translate_scalar(*scalar_expr, schema);
  if (not scalar) {
    return None{};
  }
  // `x == null` and `x != null` are null checks in TQL, which also hold for
  // values that TQL cannot decode.
  if (is<caf::none_t>(literal->value)) {
    switch (op) {
      case ast::binary_op::eq:
        return fmt::format("{} IS NULL", tql_view(*scalar));
      case ast::binary_op::neq:
        return fmt::format("{} IS NOT NULL", tql_view(*scalar));
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
/// `int64` and `double` or `int64` and `uint64`. ClickHouse would match it, so
/// such lists stay local. So does a list with a `null`, which makes TQL and
/// SQL disagree for `null` rows. For the text-like and temporal types, an
/// element that no stored value can equal is dropped; when none remains, the
/// membership is `false` for every row, `null` included, as in TQL.
auto translate_in_list(SqlScalar const& scalar, ast::list const& list)
  -> Option<std::string> {
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
  auto literals = std::vector<std::string>{};
  // Collects the literals of a text-like type, dropping those that cannot
  // match.
  auto texts = [&](auto make_literal) {
    for (auto const& constant : constants) {
      auto const* text = try_as<std::string>(constant.value);
      if (not text) {
        return false;
      }
      if (auto rendered = make_literal(*text)) {
        literals.push_back(std::move(*rendered));
      }
    }
    return true;
  };
  auto ok = match(
    scalar.type,
    [&](SqlBool const&) {
      for (auto const& constant : constants) {
        if (not is<bool>(constant.value)) {
          return false;
        }
        literals.push_back(render_constant(constant));
      }
      return true;
    },
    [&](SqlInt const&) {
      // ClickHouse converts set elements to the column type and drops
      // elements that do not convert exactly, so `int_col IN (1.5)` never
      // matches, just like `x in [1.5]` in TQL.
      for (auto const& constant : constants) {
        if (not numeric_literal_fits(true, constant)) {
          return false;
        }
        literals.push_back(render_constant(constant));
      }
      return true;
    },
    [&](SqlFloat const&) {
      for (auto const& constant : constants) {
        if (not numeric_literal_fits(false, constant)) {
          return false;
        }
        literals.push_back(render_constant(constant));
      }
      return true;
    },
    [&](SqlString const&) {
      return texts([](std::string const& x) -> Option<std::string> {
        return quote_sql_string(x);
      });
    },
    [&](SqlFixedString const& type) {
      return texts([&](std::string const& x) {
        return fixed_string_literal(x, type);
      });
    },
    [&](SqlEnum const& type) {
      return texts([&](std::string const& x) {
        return enum_literal(x, type);
      });
    },
    [&](SqlUuid const&) {
      return texts(uuid_literal);
    },
    [&](SqlTime const& type) {
      auto layout = TimeLayout::of(type);
      for (auto const& constant : constants) {
        auto const* t = try_as<time>(constant.value);
        if (not t) {
          return false;
        }
        if (auto ticks = layout.exact_tick(*t)) {
          literals.push_back(layout.literal(*ticks));
        }
      }
      return true;
    },
    [&](SqlIp const& type) {
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
    return "false";
  }
  auto set = fmt::format("({})", fmt::join(literals, ", "));
  if (scalar.nullable) {
    return fmt::format("({0} IS NOT NULL AND {0} IN {1})", scalar.sql, set);
  }
  return fmt::format("{} IN {}", scalar.sql, set);
}

auto translate_in(ast::binary_expr const& expr, SqlSchema const& schema)
  -> Option<std::string> {
  if (auto const* list = try_as<ast::list>(expr.right)) {
    auto scalar = translate_scalar(expr.left, schema);
    if (not scalar) {
      return None{};
    }
    return translate_in_list(*scalar, *list);
  }
  if (auto constant = as_literal(expr.right)) {
    if (auto const* sn = try_as<subnet>(constant->value)) {
      auto column = resolve_column(expr.left, schema);
      auto const* type = column ? try_as<SqlIp>(column->type) : nullptr;
      if (not type) {
        return None{};
      }
      return translate_ip_in_subnet(column->sql, *type, *sn);
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
  auto column = resolve_column(expr.right, schema);
  if (not column or not is<SqlString>(column->type)) {
    return None{};
  }
  return fmt::format("position({}, {}) > 0", column->sql,
                     quote_sql_string(*text));
}

// -- Predicates ---------------------------------------------------------------

/// Translates a function call that yields a boolean.
auto translate_predicate_function(ast::function_call const& call,
                                  SqlSchema const& schema)
  -> Option<std::string> {
  auto name = function_name(call);
  if (not name) {
    return None{};
  }
  if (*name == "starts_with" or *name == "ends_with") {
    // Exactly the subject and the prefix; `ignore_case` would be a third
    // argument and stays local.
    if (call.args.size() != 2) {
      return None{};
    }
    auto column = resolve_column(call.args[0], schema);
    if (not column or not is<SqlString>(column->type)) {
      return None{};
    }
    auto prefix = as_literal(call.args[1]);
    auto const* text = prefix ? try_as<std::string>(prefix->value) : nullptr;
    if (not text) {
      return None{};
    }
    return fmt::format("{}({}, {})",
                       *name == "starts_with" ? "startsWith" : "endsWith",
                       column->sql, quote_sql_string(*text));
  }
  return None{};
}

auto translate_predicate(ast::expression const& expr, SqlSchema const& schema,
                         bool positive) -> Option<std::string>;

/// Translates a boolean combinator or comparison. Both `and` and `or` keep the
/// keep-or-drop outcome of a row when an operand turns from `null` to `false`,
/// so they pass `positive` on; `not` does not.
auto translate_binary(ast::binary_expr const& expr, SqlSchema const& schema,
                      bool positive) -> Option<std::string> {
  switch (expr.op) {
    case ast::binary_op::and_:
    case ast::binary_op::or_: {
      auto left = translate_predicate(expr.left, schema, positive);
      if (not left) {
        return None{};
      }
      auto right = translate_predicate(expr.right, schema, positive);
      if (not right) {
        return None{};
      }
      auto const* keyword = expr.op == ast::binary_op::and_ ? "AND" : "OR";
      return fmt::format("({} {} {})", *left, keyword, *right);
    }
    case ast::binary_op::eq:
    case ast::binary_op::neq:
    case ast::binary_op::lt:
    case ast::binary_op::leq:
    case ast::binary_op::gt:
    case ast::binary_op::geq:
      return translate_comparison(expr, schema, positive);
    case ast::binary_op::in:
      return translate_in(expr, schema);
    default:
      return None{};
  }
}

auto translate_predicate(ast::expression const& expr, SqlSchema const& schema,
                         bool positive) -> Option<std::string> {
  return match(
    expr,
    [&](ast::binary_expr const& x) -> Option<std::string> {
      return translate_binary(x, schema, positive);
    },
    [&](ast::unary_expr const& x) -> Option<std::string> {
      if (x.op != ast::unary_op::not_) {
        return None{};
      }
      auto inner = translate_predicate(x.expr, schema, false);
      if (not inner) {
        return None{};
      }
      return fmt::format("NOT {}", *inner);
    },
    [&](ast::function_call const& x) -> Option<std::string> {
      return translate_predicate_function(x, schema);
    },
    [&](ast::root_field const&) -> Option<std::string> {
      // A bare boolean column as predicate.
      auto column = resolve_column(expr, schema);
      if (not column or not is<SqlBool>(column->type)) {
        return None{};
      }
      return column->sql;
    },
    [&](ast::field_access const&) -> Option<std::string> {
      auto column = resolve_column(expr, schema);
      if (not column or not is<SqlBool>(column->type)) {
        return None{};
      }
      return column->sql;
    },
    [](auto const&) -> Option<std::string> {
      return None{};
    });
}

// -- Schema adaptation --------------------------------------------------------

/// Returns whether `expr` names a `String` column.
auto is_string_column(ast::expression const& expr, SqlSchema const& schema)
  -> bool {
  auto column = resolve_column(expr, schema);
  return column and is<SqlString>(column->type);
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

/// Adapts an equality between a `String` column and an `ip` literal.
auto adapt_equality(ast::binary_expr& expr, SqlSchema const& schema) -> void {
  if (is_string_column(expr.left, schema)) {
    if (auto x = as_ip_literal(expr.right)) {
      replace_with_text(expr.right, *x);
    }
  } else if (is_string_column(expr.right, schema)) {
    if (auto x = as_ip_literal(expr.left)) {
      replace_with_text(expr.left, *x);
    }
  }
}

/// Adapts a membership test of a `String` column in a list of `ip` literals
/// or in a subnet.
auto adapt_membership(ast::binary_expr& expr, SqlSchema const& schema) -> void {
  if (not is_string_column(expr.left, schema)) {
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

/// Resolves `ip(column)` on a `String` column to its ClickHouse counterpart.
/// The result is nullable even for a non-nullable column, since the parse may
/// fail.
auto resolve_parsed_ip(ast::expression const& expr, SqlSchema const& schema)
  -> Option<SqlScalar> {
  auto const* call = try_as<ast::function_call>(expr);
  if (not call or call->args.size() != 1) {
    return None{};
  }
  auto name = function_name(*call);
  if (not name or *name != "ip") {
    return None{};
  }
  auto column = resolve_column(call->args.front(), schema);
  if (not column or not is<SqlString>(column->type)) {
    return None{};
  }
  return SqlScalar{.sql = fmt::format("toIPv6OrNull({})", column->sql),
                   .type = SqlIp{.v4 = false},
                   .nullable = true};
}

/// Keeps the rows that ClickHouse cannot parse, so that only the strings both
/// parsers accept need to agree.
auto keep_unparsed(SqlScalar const& parsed, std::string const& condition)
  -> std::string {
  return fmt::format("({} IS NULL OR {})", parsed.sql, condition);
}

/// Translates a prefilter for a comparison that involves `ip(column)`.
///
/// Where TQL's `ip` yields a value, it is `null` for a string that does not
/// parse, and TQL's `!=` is `true` for `null`. A row that ClickHouse parses
/// but TQL does not would then be kept by TQL and dropped by the prefilter,
/// so `!=` gets none. Every other comparison is `true` only where TQL parsed
/// the string, and ClickHouse then either agrees or did not parse it.
auto translate_parsed_ip_comparison(ast::binary_expr const& expr,
                                    SqlSchema const& schema)
  -> Option<std::string> {
  if (expr.op == ast::binary_op::neq) {
    return None{};
  }
  auto op = expr.op;
  auto parsed = resolve_parsed_ip(expr.left, schema);
  auto literal = as_ip_literal(expr.right);
  if (not parsed or not literal) {
    parsed = resolve_parsed_ip(expr.right, schema);
    literal = as_ip_literal(expr.left);
    if (not parsed or not literal) {
      return None{};
    }
    op = flip_comparison(op);
  }
  return keep_unparsed(*parsed, translate_ip_comparison(parsed->sql, false,
                                                        SqlIp{.v4 = false}, op,
                                                        *literal));
}

/// Translates a prefilter for `ip(column) in list` or `ip(column) in subnet`.
auto translate_parsed_ip_membership(ast::binary_expr const& expr,
                                    SqlSchema const& schema)
  -> Option<std::string> {
  auto parsed = resolve_parsed_ip(expr.left, schema);
  if (not parsed) {
    return None{};
  }
  if (auto const* list = try_as<ast::list>(expr.right)) {
    auto plain = SqlScalar{parsed->sql, parsed->type, false};
    auto set = translate_in_list(plain, *list);
    if (not set) {
      return None{};
    }
    return keep_unparsed(*parsed, *set);
  }
  auto literal = as_literal(expr.right);
  auto const* sn = literal ? try_as<subnet>(literal->value) : nullptr;
  if (not sn) {
    return None{};
  }
  return keep_unparsed(
    *parsed, translate_ip_in_subnet(parsed->sql, SqlIp{.v4 = false}, *sn));
}

/// Appends the conjuncts of `expr` to `out`, splitting nested `and`s.
///
/// Splitting lets a translatable conjunct go into SQL while an earlier one
/// stays local, so the local one then sees only the rows that survived the
/// pushed one. This is safe because a conjunction selects the same rows in any
/// order and `where` predicates are pure. It only changes how often a local
/// predicate is evaluated, and with it the number of warnings it emits for
/// rows that the result never contains. TQL's `and` already evaluates its right
/// side only on rows the left side did not decide, and `export` splits a
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

// -- SqlSchema ----------------------------------------------------------------

auto SqlSchema::make_node(std::string_view type) -> Node {
  auto node = Node{.type = std::string{type}, .column = None{}, .children = {}};
  auto nullable = false;
  auto bare = unwrap_column_type(type, nullable);
  if (auto parsed = parse_type(bare)) {
    node.column = SqlColumn{.type = std::move(*parsed), .nullable = nullable};
    return node;
  }
  auto elements = unwrap_clickhouse_type_call(bare, "Tuple");
  if (not elements) {
    return node;
  }
  for (auto element : split_top_level_clickhouse_type_arguments(*elements)) {
    auto split = find_top_level_clickhouse_type_space(element);
    // Unnamed tuple elements are only addressable by index in SQL.
    if (split == std::string_view::npos) {
      continue;
    }
    auto name
      = unquote_identifier_component(detail::trim(element.substr(0, split)));
    node.children.emplace(std::move(name), Box<Node>{make_node(detail::trim(
                                             element.substr(split + 1)))});
  }
  return node;
}

auto SqlSchema::add_column(std::string_view name, std::string_view type,
                           bool generated) -> void {
  columns_.emplace_back(name);
  if (generated) {
    generated_.emplace(std::string{name});
    return;
  }
  auto normalized = remove_non_significant_whitespace(type);
  if (not is_decodable_type(normalized)) {
    return;
  }
  nodes_.emplace(std::string{name}, make_node(normalized));
}

auto SqlSchema::is_generated(std::string_view name) const -> bool {
  return generated_.contains(std::string{name});
}

auto SqlSchema::columns() const -> std::span<const std::string> {
  return columns_;
}

auto SqlSchema::find_node(std::span<const std::string> path) const
  -> Node const* {
  if (path.empty()) {
    return nullptr;
  }
  auto it = nodes_.find(path.front());
  if (it == nodes_.end()) {
    return nullptr;
  }
  auto const* node = &it->second;
  for (auto const& segment : path.subspan(1)) {
    auto child = node->children.find(segment);
    if (child == node->children.end()) {
      return nullptr;
    }
    node = &*child->second;
  }
  return node;
}

auto SqlSchema::find(std::span<const std::string> path) const
  -> Option<SqlColumn> {
  auto const* node = find_node(path);
  if (not node) {
    return None{};
  }
  return node->column;
}

namespace {

/// The tuple elements that a projection requests below a node.
struct Requested {
  detail::flat_map<std::string, Box<Requested>> children;
  /// Whether the node itself is requested, which subsumes its elements.
  bool whole = false;
};

} // namespace

auto SqlSchema::render_projection(
  std::string_view name, std::span<const std::vector<std::string>> paths) const
  -> Option<std::string> {
  auto const* root = find_node(std::array{std::string{name}});
  if (not root or root->children.empty()) {
    return None{};
  }
  auto requested = Requested{};
  for (auto const& path : paths) {
    auto* node = root;
    auto* request = &requested;
    for (auto const& segment : path) {
      auto child = node->children.find(segment);
      // A path outside the schema selects the column whole; the local
      // `select` then yields `null` for it either way.
      if (child == node->children.end()) {
        return None{};
      }
      node = &*child->second;
      auto next = request->children.find(segment);
      if (next == request->children.end()) {
        next = request->children.emplace(segment, Box<Requested>{std::in_place})
                 .first;
      }
      request = &*next->second;
    }
    request->whole = true;
  }
  if (requested.whole) {
    return None{};
  }
  // Rebuilds the tuple below `node` with the requested elements only, in
  // declaration order, and casts it back to a named tuple so that it decodes
  // to a record.
  auto render
    = [](this auto const& self, Node const& node, Requested const& request,
         std::string const& sql) -> std::pair<std::string, std::string> {
    if (request.whole or node.children.empty()) {
      return {sql, node.type};
    }
    auto values = std::vector<std::string>{};
    auto elements = std::vector<std::string>{};
    for (auto const& [child_name, child] : node.children) {
      auto it = request.children.find(child_name);
      if (it == request.children.end()) {
        continue;
      }
      auto quoted = quote_sql_identifier(child_name);
      auto [value, type]
        = self(*child, *it->second, fmt::format("{}.{}", sql, quoted));
      values.push_back(std::move(value));
      elements.push_back(fmt::format("{} {}", quoted, type));
    }
    auto type = fmt::format("Tuple({})", fmt::join(elements, ", "));
    return {fmt::format("CAST(tuple({}), {})", fmt::join(values, ", "),
                        quote_sql_string(type)),
            std::move(type)};
  };
  auto quoted = quote_sql_identifier(name);
  return fmt::format("{} AS {}", render(*root, requested, quoted).first,
                     quoted);
}

// -- Entry points -------------------------------------------------------------

auto adapt_to_schema(ast::expression& expr, SqlSchema const& schema) -> void {
  match(
    expr,
    [&](ast::binary_expr& x) {
      switch (x.op) {
        case ast::binary_op::and_:
        case ast::binary_op::or_:
          adapt_to_schema(x.left, schema);
          adapt_to_schema(x.right, schema);
          return;
        case ast::binary_op::eq:
        case ast::binary_op::neq:
          adapt_equality(x, schema);
          return;
        case ast::binary_op::in:
          adapt_membership(x, schema);
          return;
        default:
          return;
      }
    },
    [&](ast::unary_expr& x) {
      if (x.op == ast::unary_op::not_) {
        adapt_to_schema(x.expr, schema);
      }
    },
    [](auto&) {});
}

auto split_filter_for_sql(ir::OptimizeFilter filter, SqlSchema const& schema)
  -> SqlFilterSplit {
  auto conjuncts = ir::OptimizeFilter{};
  for (auto& expr : filter) {
    adapt_to_schema(expr, schema);
    collect_conjuncts(std::move(expr), conjuncts);
  }
  auto result = SqlFilterSplit{};
  for (auto& expr : conjuncts) {
    if (auto sql = translate_predicate(expr, schema)) {
      result.pushed.push_back(std::move(*sql));
      continue;
    }
    if (auto sql = translate_prefilter(expr, schema)) {
      result.pushed.push_back(std::move(*sql));
    }
    result.remaining.push_back(std::move(expr));
  }
  return result;
}

auto translate_prefilter(ast::expression const& expr, SqlSchema const& schema)
  -> Option<std::string> {
  // An exact translation is the tightest prefilter. It may use the positive
  // form, since a prefilter drops rows on `false` and `null` alike.
  if (auto exact = translate_predicate(expr, schema, true)) {
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
      auto left = translate_prefilter(binary->left, schema);
      auto right = translate_prefilter(binary->right, schema);
      if (left and right) {
        return fmt::format("({} AND {})", *left, *right);
      }
      return left ? left : right;
    }
    case ast::binary_op::or_: {
      auto left = translate_prefilter(binary->left, schema);
      if (not left) {
        return None{};
      }
      auto right = translate_prefilter(binary->right, schema);
      if (not right) {
        return None{};
      }
      return fmt::format("({} OR {})", *left, *right);
    }
    case ast::binary_op::eq:
    case ast::binary_op::neq:
    case ast::binary_op::lt:
    case ast::binary_op::leq:
    case ast::binary_op::gt:
    case ast::binary_op::geq:
      return translate_parsed_ip_comparison(*binary, schema);
    case ast::binary_op::in:
      return translate_parsed_ip_membership(*binary, schema);
    default:
      return None{};
  }
}

auto translate_predicate(ast::expression const& expr, SqlSchema const& schema)
  -> Option<std::string> {
  // A predicate at the top of the filter chain drops a row on `false` and on
  // `null` alike.
  return translate_predicate(expr, schema, true);
}

auto make_select_query(std::string_view table, SqlSchema const* schema,
                       Option<ir::OptimizeProjection> const& projection,
                       std::span<const std::string> where,
                       Option<uint64_t> limit) -> std::string {
  auto columns = std::string{"*"};
  if (schema and projection) {
    // The nested paths requested below each top-level column, relative to it.
    auto wanted
      = detail::flat_map<std::string, std::vector<std::vector<std::string>>>{};
    for (auto const& path : *projection) {
      auto segments = path.path();
      if (segments.empty()) {
        continue;
      }
      auto relative = std::vector<std::string>{};
      for (auto const& segment : segments.subspan(1)) {
        relative.push_back(segment.id.name);
      }
      wanted[segments.front().id.name].push_back(std::move(relative));
    }
    // Whether `SELECT *` contains a generated column depends on session
    // settings, so a projection that names one keeps `*` to match either way.
    auto names_generated = std::ranges::any_of(wanted, [&](auto const& entry) {
      return schema->is_generated(entry.first);
    });
    auto selected = std::vector<std::string>{};
    for (auto const& column : schema->columns()) {
      auto it = wanted.find(column);
      if (it == wanted.end()) {
        continue;
      }
      if (auto narrowed = schema->render_projection(column, it->second)) {
        selected.push_back(std::move(*narrowed));
      } else {
        selected.push_back(quote_sql_identifier(column));
      }
    }
    if (not names_generated and not selected.empty()) {
      columns = fmt::format("{}", fmt::join(selected, ", "));
    }
  }
  auto query = fmt::format("SELECT {} FROM {}", columns, table);
  if (not where.empty()) {
    fmt::format_to(std::back_inserter(query), " WHERE {}",
                   fmt::join(where, " AND "));
  }
  if (limit) {
    fmt::format_to(std::back_inserter(query), " LIMIT {}", *limit);
  }
  return query;
}

auto quote_sql_identifier(std::string_view name) -> std::string {
  auto result = std::string{"`"};
  for (auto c : name) {
    if (c == '`' or c == '\\') {
      result += '\\';
    }
    result += c;
  }
  result += '`';
  return result;
}

auto quote_sql_string(std::string_view text) -> std::string {
  // Backslash-escapes the quote and the backslash, and hex-escapes control
  // bytes; every other byte, including non-ASCII, passes through.
  auto escaper = [](auto& f, auto out) {
    auto byte = static_cast<unsigned char>(*f);
    if (*f == '\'' or *f == '\\') {
      *out++ = '\\';
      *out++ = *f++;
    } else if (byte < 0x20 or byte == 0x7f) {
      detail::hex_escaper(f, out);
    } else {
      *out++ = *f++;
    }
  };
  return fmt::format("'{}'", detail::escape(text, escaper));
}

} // namespace tenzir::plugins::clickhouse
