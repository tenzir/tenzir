//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/sql_pushdown.hpp"

#include "clickhouse/arguments.hpp"
#include "clickhouse/block_decoding.hpp"
#include "clickhouse/transformers.hpp"
#include "tenzir/checked_math.hpp"
#include "tenzir/concepts.hpp"
#include "tenzir/detail/escapers.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/pushdown/aggregation.hpp"
#include "tenzir/pushdown/translate.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/try.hpp"

#include <clickhouse/columns/factory.h>
#include <clickhouse/types/types.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>

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
auto parse_enum(std::string_view type) -> Option<pushdown::ColumnType> {
  auto column = ::clickhouse::CreateColumnByType(std::string{type});
  auto const* enum_type
    = column ? column->Type()->As<::clickhouse::EnumType>() : nullptr;
  if (not enum_type) {
    return None{};
  }
  auto result = pushdown::EnumType{};
  for (auto it = enum_type->BeginValueToName();
       it != enum_type->EndValueToName(); ++it) {
    result.names.emplace(it->second);
  }
  return result;
}

constexpr auto nanos_per_second = int64_t{1'000'000'000};
constexpr auto nanos_per_day = nanos_per_second * 86'400;

/// Describes the tick grid of a temporal type, or returns `None` if `type` is
/// not one. `from_clickhouse` decodes every temporal type to nanoseconds since
/// the epoch.
auto parse_time_type(std::string_view type) -> Option<pushdown::TimeType> {
  constexpr auto max = std::numeric_limits<int64_t>::max();
  if (type == "Date") {
    // `UInt16` days, 1970-01-01 to 2149-06-06, all decodable.
    return pushdown::TimeType{
      .native_type = "Date",
      .unit = nanos_per_day,
      .lo = 0,
      .hi = 65535,
      .guard_lo = false,
      .guard_hi = false,
    };
  }
  if (type == "Date32") {
    // `Int32` days, 1900-01-01 to 2299-12-31. Days past 2262-04-11 do not fit
    // into nanoseconds. ClickHouse saturates literals before 1900 and none of
    // its functions produce such days, so no lower guard.
    return pushdown::TimeType{
      .native_type = "Date32",
      .unit = nanos_per_day,
      .lo = -25567,
      .hi = max / nanos_per_day,
      .guard_lo = false,
      .guard_hi = true,
    };
  }
  // The time zone argument of `DateTime` and `DateTime64` only affects how
  // ClickHouse renders the value.
  if (auto args = unwrap_clickhouse_type_call(type, "DateTime64")) {
    auto list = split_top_level_clickhouse_type_arguments(*args);
    auto precision = size_t{};
    if (list.empty() or not parse_clickhouse_size(list.front(), precision)
        or precision > 9) {
      return None{};
    }
    // `Int64` ticks of `10^-precision` seconds. Only at nanosecond precision
    // does every tick fit; coarser precisions reach past 2262.
    auto unit = checked_pow(int64_t{10}, 9 - precision);
    TENZIR_ASSERT(unit);
    auto hi = max / *unit;
    // `-2^63` is not a multiple of ten, so its floor and `-hi` coincide.
    auto lo = *unit == 1 ? std::numeric_limits<int64_t>::min() : -hi;
    auto guarded = precision < 9;
    return pushdown::TimeType{
      .native_type = fmt::format("DateTime64({})", precision),
      .unit = *unit,
      .lo = lo,
      .hi = hi,
      .guard_lo = guarded,
      .guard_hi = guarded,
    };
  }
  if (type == "DateTime" or unwrap_clickhouse_type_call(type, "DateTime")) {
    // `UInt32` seconds, all decodable.
    return pushdown::TimeType{
      .native_type = "DateTime",
      .unit = nanos_per_second,
      .lo = 0,
      .hi = 4294967295,
      .guard_lo = false,
      .guard_hi = false,
    };
  }
  return None{};
}

/// Maps a bare ClickHouse type to the description of how TQL sees its values.
/// Types that TQL decodes ambiguously (e.g. `Decimal` scaling) or not at all
/// are left out.
auto parse_type(std::string_view type) -> Option<pushdown::ColumnType> {
  constexpr auto integers
    = std::array<std::pair<std::string_view, pushdown::IntType>, 8>{{
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
    return pushdown::BoolType{};
  }
  if (type == "String") {
    return pushdown::StringType{};
  }
  for (auto const& [name, info] : integers) {
    if (type == name) {
      return info;
    }
  }
  if (type == "Float32" or type == "Float64") {
    return pushdown::FloatType{};
  }
  if (type == "UUID") {
    return pushdown::UuidType{};
  }
  if (type == "IPv4") {
    return pushdown::IpType{.v4 = true};
  }
  if (type == "IPv6") {
    return pushdown::IpType{.v4 = false};
  }
  if (auto time = parse_time_type(type)) {
    return std::move(*time);
  }
  if (auto args = unwrap_clickhouse_type_call(type, "FixedString")) {
    auto length = size_t{};
    if (not parse_clickhouse_size(*args, length)) {
      return None{};
    }
    return pushdown::FixedStringType{.length = length};
  }
  for (auto name : {"Enum8"sv, "Enum16"sv, "Enum"sv}) {
    if (unwrap_clickhouse_type_call(type, name)) {
      return parse_enum(type);
    }
  }
  return None{};
}

// -- Literals -----------------------------------------------------------------

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

/// The function that yields the stored tick count of a column of `type`, or
/// `None` if there is none. The tick count is a plain `Int64` with which a
/// guard can compare, and it is `NULL` for a `NULL` value.
///
/// The `toUnixTimestamp64*` functions convert to their own unit, so only the
/// one at the column's precision returns the tick count unchanged. Another one
/// either throws when the conversion overflows or truncates, which ruins the
/// guard's exactness at the edge of the range. `reinterpretAsInt64` returns
/// the tick count at every precision.
auto tick_count_function(pushdown::TimeType const& type)
  -> Option<std::string_view> {
  if (not type.native_type.starts_with("DateTime64(")) {
    return None{};
  }
  switch (type.unit) {
    case 1'000'000:
      return "toUnixTimestamp64Milli";
    case 1'000:
      return "toUnixTimestamp64Micro";
    case 1:
      return "toUnixTimestamp64Nano";
    default:
      return "reinterpretAsInt64";
  }
}

/// Spells a comparison operator, or returns `None` for another operator.
auto comparison_operator(pushdown::BinaryOp op) -> Option<std::string_view> {
  switch (op) {
    case pushdown::BinaryOp::eq:
      return "=";
    case pushdown::BinaryOp::lt:
      return "<";
    case pushdown::BinaryOp::leq:
      return "<=";
    case pushdown::BinaryOp::gt:
      return ">";
    case pushdown::BinaryOp::geq:
      return ">=";
    default:
      return None{};
  }
}

/// Whether `toString` renders `text` verbatim inside a list element, which
/// it quotes and escapes.
auto is_verbatim_in_list(std::string_view text) -> bool {
  return std::ranges::none_of(text, [](char c) {
    auto const byte = static_cast<unsigned char>(c);
    return c == '\'' or c == '\\' or byte < 0x20 or byte == 0x7f;
  });
}

/// Whether `type` is `JSON` without typed or skipped paths, whose values
/// ClickHouse spreads over a path for each value below an object. A typed
/// path may hold a `Map`, a `Tuple`, or a `Dynamic` with an object, which TQL
/// sees as a record without ClickHouse having paths below it.
auto is_untyped_json(std::string_view type) -> bool {
  auto const normalized = remove_non_significant_whitespace(type);
  auto bare = std::string_view{normalized};
  if (auto inner = unwrap_clickhouse_type_call(bare, "Nullable")) {
    bare = *inner;
  }
  if (bare == "JSON") {
    return true;
  }
  auto args = unwrap_clickhouse_type_call(bare, "JSON");
  if (not args) {
    return false;
  }
  return std::ranges::all_of(split_top_level_clickhouse_type_arguments(*args),
                             [](std::string_view x) {
                               return x.starts_with("max_dynamic_paths=")
                                      or x.starts_with("max_dynamic_types=");
                             });
}

/// Collects the paths of the fields that an expression references, outside
/// of lambdas, whose parameters shadow fields.
class FieldPathCollector final : public ast::visitor<FieldPathCollector> {
public:
  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::expression& x) -> void {
    // Only these can be a non-empty path, and `try_from` copies its input.
    auto const candidate = is<ast::root_field>(x) or is<ast::field_access>(x)
                           or is<ast::index_expr>(x);
    if (auto path = candidate ? ast::field_path::try_from(x) : None{};
        path and not path->path().empty()) {
      auto segments = std::vector<std::string>{};
      for (auto const& segment : path->path()) {
        segments.push_back(segment.id.name);
      }
      paths.push_back(std::move(segments));
      return;
    }
    enter(x);
  }

  auto visit(ast::lambda_expr&) -> void {
  }

  std::vector<std::vector<std::string>> paths;
};

} // namespace

// -- ClickHouseRenderer -------------------------------------------------------

auto ClickHouseRenderer::quote_identifier(std::string_view name) const
  -> std::string {
  return quote_sql_identifier(name);
}

auto ClickHouseRenderer::quote_string(std::string_view text) const
  -> std::string {
  return quote_sql_string(text);
}

auto ClickHouseRenderer::render_double(double x) const -> std::string {
  // `{}` picks the shortest representation that round-trips, but renders an
  // integral value without a fraction. ClickHouse would read that as an
  // integer and, in arithmetic with an integer column, compute exactly where
  // TQL computes in `double`. A trailing `.` keeps it a `Float64`.
  auto result = fmt::format("{}", x);
  if (result.find_first_of(".e") == std::string::npos) {
    result += '.';
  }
  return result;
}

auto ClickHouseRenderer::render_enum(pushdown::EnumLabel const& x) const
  -> Option<Fragment> {
  // A label is the server's own rendering between quotes (see `parse_enum`),
  // so quoting it verbatim yields the literal that ClickHouse parses back to
  // the label.
  return sql_atom(fmt::format("'{}'", x.name));
}

auto ClickHouseRenderer::render_time(pushdown::TimeValue const& x) const
  -> Option<Fragment> {
  // Every form avoids the session time zone: date strings and the
  // `fromUnixTimestamp64*` family denote an instant regardless of it, and an
  // explicit `'UTC'` fixes the `DateTime` parse. `toDateTime64` from a string
  // would be the natural choice for `DateTime64`, but it saturates before
  // 1900, so the literal is built from its integer tick count instead.
  auto const& type = x.type;
  TENZIR_ASSERT(x.ticks >= type.lo and x.ticks <= type.hi);
  // Calls `name` on atoms.
  auto call = [&](std::string_view name, auto... args) {
    return sql_call(name, std::array{sql_atom(std::move(args))...});
  };
  if (type.native_type == "Date") {
    return call("toDate", quote_string(format_date(x.ticks)));
  }
  if (type.native_type == "Date32") {
    return call("toDate32", quote_string(format_date(x.ticks)));
  }
  if (type.native_type == "DateTime") {
    auto [days, seconds] = floor_div(x.ticks, 86'400);
    return call("toDateTime",
                quote_string(fmt::format("{} {:02}:{:02}:{:02}",
                                         format_date(days), seconds / 3600,
                                         seconds / 60 % 60, seconds % 60)),
                quote_string("UTC"));
  }
  if (not type.native_type.starts_with("DateTime64(")) {
    return None{};
  }
  switch (type.unit) {
    case 1'000'000:
      return call("fromUnixTimestamp64Milli", fmt::to_string(x.ticks));
    case 1'000:
      return call("fromUnixTimestamp64Micro", fmt::to_string(x.ticks));
    case 1:
      return call("fromUnixTimestamp64Nano", fmt::to_string(x.ticks));
    default:
      // The tick is within `[lo, hi]`, so its nanoseconds fit.
      return sql_call("CAST",
                      std::array{call("fromUnixTimestamp64Nano",
                                      fmt::to_string(x.ticks * type.unit)),
                                 sql_atom(quote_string(type.native_type))});
  }
}

auto ClickHouseRenderer::render_ip(pushdown::IpValue const& x) const
  -> Option<Fragment> {
  // TQL prints IPv4-mapped addresses in dotted form, which `toIPv6` maps back
  // to the same 16 bytes.
  auto address = sql_atom(quote_string(fmt::to_string(x.address)));
  return sql_call(x.v4 ? "toIPv4" : "toIPv6", std::array{std::move(address)});
}

auto ClickHouseRenderer::render_column(std::span<std::string const> path) const
  -> Option<Fragment> {
  if (schema_) {
    if (auto field = schema_->json_field(path)) {
      // The text that ClickHouse sends for the value in JSON, and an empty
      // string for a missing one.
      return sql_call("toString", std::array{render_json_value(*field)});
    }
    if (auto column = schema_->placed_column(path)) {
      return sql_atom(quote_sql_identifier(*column));
    }
  }
  return SqlRenderer::render_column(path);
}

auto ClickHouseRenderer::json_field(pushdown::Expr const& x) const
  -> Option<SqlSchema::JsonField const&> {
  auto const* column = try_as<pushdown::Column>(x);
  if (not schema_ or not column) {
    return None{};
  }
  return schema_->json_field(column->path);
}

auto ClickHouseRenderer::render_json_value(
  SqlSchema::JsonField const& field) const -> Fragment {
  // Unlike `column.path`, `getSubcolumn` never confuses a dotted column name
  // with a path below another column, and it reads every key literally.
  return sql_call("getSubcolumn",
                  std::array{sql_atom(quote_sql_identifier(field.column)),
                             sql_atom(quote_sql_string(field.subcolumn))});
}

auto ClickHouseRenderer::render_json_comparison(
  pushdown::Binary const& x, Fragment const& text,
  pushdown::Literal const& literal, Fragment const& rendered) const
  -> Option<Fragment> {
  // TQL's `!=` also holds for a value of another type, whose text may equal
  // the literal's, so it gets no prefilter.
  TRY(auto op, comparison_operator(x.op));
  auto const predicate = Fragment::Kind::predicate;
  return match(
    literal.value,
    [&](std::string const&) -> Option<Fragment> {
      // TQL sees a string only where ClickHouse sends a JSON string, whose
      // text is the stored value's. Both compare bytes.
      return sql_binary(op, text, rendered, predicate);
    },
    [&](bool value) -> Option<Fragment> {
      // TQL sees `true` for a JSON `true` and for the string `"true"`, and
      // both have the text `true`.
      if (x.op != pushdown::BinaryOp::eq) {
        return None{};
      }
      return sql_binary(op, text,
                        sql_atom(quote_sql_string(value ? "true" : "false")),
                        predicate);
    },
    [&]<class T>(T const&) -> Option<Fragment> {
      if constexpr (concepts::one_of<T, int64_t, uint64_t, double>) {
        // TQL sees a number only where ClickHouse sends a JSON number, whose
        // text parses to the same `double`, or for an integer, rounds to it.
        // The rounding only matters at `2^53`, which translation accounts
        // for. Parsing text that is not a number yields `NULL`.
        return sql_binary(op, sql_call("toFloat64OrNull", std::array{text}),
                          rendered, predicate);
      } else {
        return None{};
      }
    });
}

auto ClickHouseRenderer::render_call(pushdown::Call const& x,
                                     std::span<Fragment const> args) const
  -> Option<Fragment> {
  for (auto i = size_t{0}; i < x.args.size(); ++i) {
    if (not json_field(x.args[i])) {
      continue;
    }
    // The subject of an affix or substring search is the text of the value.
    if (i != 0) {
      return None{};
    }
    if (x.op == pushdown::Operation::contains) {
      // TQL's `needle in x` also searches the elements of a list, whose text
      // contains every element verbatim, unless it needed escaping.
      auto const* literal
        = x.args.size() == 2 ? try_as<pushdown::Literal>(x.args[1]) : nullptr;
      auto const* needle
        = literal ? try_as<std::string>(literal->value) : nullptr;
      if (not needle or not is_verbatim_in_list(*needle)) {
        return None{};
      }
    } else if (x.op != pushdown::Operation::starts_with
               and x.op != pushdown::Operation::ends_with) {
      return None{};
    }
  }
  switch (x.op) {
    case pushdown::Operation::starts_with:
      TENZIR_ASSERT(args.size() == 2);
      return sql_call("startsWith", args);
    case pushdown::Operation::ends_with:
      TENZIR_ASSERT(args.size() == 2);
      return sql_call("endsWith", args);
    case pushdown::Operation::contains:
      TENZIR_ASSERT(args.size() == 2);
      return sql_binary(">", sql_call("position", args), sql_atom("0"),
                        Fragment::Kind::predicate);
    case pushdown::Operation::length_bytes:
      TENZIR_ASSERT(args.size() == 1);
      return sql_call("length", args);
    case pushdown::Operation::parse_ip:
      // Maps IPv4 text into IPv6, and yields `NULL` for text it cannot parse.
      TENZIR_ASSERT(args.size() == 1);
      return sql_call("toIPv6OrNull", args);
    case pushdown::Operation::fold_case:
      // Lowercases per code point, so `ß` stays `ß` where TQL folds it to
      // `ss`.
      TENZIR_ASSERT(args.size() == 1);
      return sql_call("lowerUTF8", args);
    case pushdown::Operation::match_regex: {
      // `match` searches anywhere in the string with RE2, like TQL, but lets
      // `.` match a newline unless the pattern disables that with `(?-s)`.
      // It rejects patterns with NUL bytes, and its behavior on invalid UTF-8
      // is undefined, which the IR accepts.
      TENZIR_ASSERT(args.size() == 2);
      auto const* literal = try_as<pushdown::Literal>(x.args[1]);
      auto const* pattern
        = literal ? try_as<std::string>(literal->value) : nullptr;
      if (not pattern or pattern->find('\0') != std::string::npos) {
        return None{};
      }
      return sql_call("match",
                      std::array{args[0],
                                 sql_atom(quote_string("(?-s)" + *pattern))});
    }
  }
  TENZIR_UNREACHABLE();
}

auto ClickHouseRenderer::render_comparison(pushdown::Binary const& x,
                                           Fragment const& left,
                                           Fragment const& right) const
  -> Option<Fragment> {
  if (json_field(*x.right)) {
    return None{};
  }
  if (json_field(*x.left)) {
    auto const* literal = try_as<pushdown::Literal>(*x.right);
    if (not literal) {
      return None{};
    }
    return render_json_comparison(x, left, *literal, right);
  }
  if (target_ != SqlTarget::hydrolix
      or x.origin != pushdown::ComparisonOrigin::time_guard) {
    return SqlRenderer::render_comparison(x, left, right);
  }
  // Hydrolix reads a time range from a guard on its bare primary column,
  // which then spans past 2262 or before 1677. A query option such as
  // `hdx_query_max_timerange_sec` rejects the upper bound, and the lower one
  // overflows Hydrolix's partition catalog, which then finds no rows. A guard
  // on the column's tick count means the same, but Hydrolix's range analysis
  // skips it. Other temporal types cannot be Hydrolix's primary timestamp,
  // so their guards stay bare.
  auto const& literal = as<pushdown::Literal>(*x.right);
  auto const& value = as<pushdown::TimeValue>(literal.value);
  TENZIR_ASSERT(x.op == pushdown::BinaryOp::geq
                or x.op == pushdown::BinaryOp::leq);
  auto ticks = tick_count_function(value.type);
  if (not ticks) {
    return SqlRenderer::render_comparison(x, left, right);
  }
  return sql_binary(x.op == pushdown::BinaryOp::geq ? ">=" : "<=",
                    sql_call(*ticks, std::array{left}),
                    sql_atom(fmt::to_string(value.ticks)),
                    Fragment::Kind::predicate);
}

auto ClickHouseRenderer::render_arithmetic(pushdown::Binary const& x,
                                           Fragment const& left,
                                           Fragment const& right) const
  -> Option<Fragment> {
  if (json_field(*x.left) or json_field(*x.right)) {
    return None{};
  }
  return SqlRenderer::render_arithmetic(x, left, right);
}

auto ClickHouseRenderer::render_in(pushdown::In const& x, Fragment const& expr,
                                   std::span<Fragment const> list) const
  -> Option<Fragment> {
  if (not json_field(*x.expr)) {
    return SqlRenderer::render_in(x, expr, list);
  }
  // The elements may differ in type, so each one compares on its own.
  auto equalities = std::vector<Fragment>{};
  for (auto i = size_t{0}; i < x.list.size(); ++i) {
    auto const* literal = try_as<pushdown::Literal>(x.list[i]);
    if (not literal) {
      return None{};
    }
    auto equality = pushdown::Binary{
      .op = pushdown::BinaryOp::eq,
      .left = *x.expr,
      .right = x.list[i],
    };
    TRY(auto fragment,
        render_json_comparison(equality, expr, *literal, list[i]));
    equalities.push_back(std::move(fragment));
  }
  if (equalities.size() == 1) {
    return std::move(equalities.front());
  }
  return sql_junction("OR", equalities);
}

auto ClickHouseRenderer::render_is_null(pushdown::IsNull const& x,
                                        Fragment const& operand) const
  -> Option<Fragment> {
  auto field = json_field(*x.expr);
  if (not field) {
    return SqlRenderer::render_is_null(x, operand);
  }
  // A missing field and a JSON `null` have no text of their own.
  if (not x.negated) {
    return None{};
  }
  // ClickHouse stores no path for an object, but one for each value below
  // it. An empty object or one with only nulls has none, and ClickHouse sends
  // it as missing.
  auto value = sql_is_null(render_json_value(*field), true);
  auto prefix = quote_sql_string(fmt::format("{}.", field->subcolumn));
  auto below = sql_call(
    "arrayExists",
    std::array{
      sql_atom(
        fmt::format("__tenzir_path -> startsWith(__tenzir_path, {})", prefix)),
      sql_call("JSONAllPaths",
               std::array{sql_atom(quote_sql_identifier(field->column))}),
    });
  return sql_junction("OR", std::array{std::move(value), std::move(below)});
}

auto ClickHouseRenderer::render_conditional(Fragment const& condition,
                                            Fragment const& then,
                                            Fragment const& otherwise) const
  -> Option<Fragment> {
  return sql_call("if", std::array{condition, then, otherwise});
}

// -- SqlSchema ----------------------------------------------------------------

auto SqlSchema::make_node(std::string_view type, std::vector<std::string>& path)
  -> Node {
  auto node = Node{.type = std::string{type}, .children = {}};
  auto nullable = false;
  auto bare = unwrap_column_type(type, nullable);
  if (auto parsed = parse_type(bare)) {
    model_.add(path, {.type = std::move(*parsed), .nullable = nullable});
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
    if (node.children.contains(name)) {
      continue;
    }
    path.push_back(name);
    auto child = make_node(detail::trim(element.substr(split + 1)), path);
    path.pop_back();
    node.children.emplace(std::move(name), Box<Node>{std::move(child)});
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
  if (not is_decodable_type(normalized) or nodes_.contains(name)) {
    return;
  }
  auto path = std::vector<std::string>{std::string{name}};
  nodes_.emplace(std::string{name}, make_node(normalized, path));
}

auto SqlSchema::add_placed_column(std::string_view name, std::string_view type,
                                  std::vector<std::string> path) -> void {
  TENZIR_ASSERT(not path.empty());
  auto normalized = remove_non_significant_whitespace(type);
  if (not is_decodable_type(normalized) or placed_.contains(path)) {
    return;
  }
  auto nullable = false;
  auto parsed = parse_type(unwrap_column_type(normalized, nullable));
  if (not parsed) {
    return;
  }
  model_.add(path, {.type = std::move(*parsed), .nullable = nullable});
  placed_.emplace(std::move(path), std::string{name});
}

auto SqlSchema::add_json_field(std::vector<std::string> path,
                               std::string_view column,
                               std::span<const std::string> json_path) -> void {
  TENZIR_ASSERT(not path.empty());
  if (json_path.empty() or json_fields_.contains(path)
      or std::ranges::any_of(json_path, &std::string::empty)) {
    return;
  }
  // `json_type_escape_dots_in_keys=1` stores a key `a.b` at the path `a%2Eb`,
  // and turns `%2E` back into `.` when ClickHouse sends it. A key with a
  // literal `%2E` thus reaches TQL with a `.`, and reads from the same path.
  auto escaped = std::vector<std::string>{};
  for (auto const& key : json_path) {
    escaped.push_back(detail::replace_all(key, ".", "%2E"));
  }
  model_.add(path, {.type = pushdown::DynamicType{}, .nullable = true});
  json_fields_.emplace(std::move(path),
                       JsonField{
                         .column = std::string{column},
                         .subcolumn = fmt::to_string(fmt::join(escaped, ".")),
                       });
}

auto SqlSchema::json_field(std::span<const std::string> path) const
  -> Option<JsonField const&> {
  auto it
    = json_fields_.find(std::vector<std::string>{path.begin(), path.end()});
  if (it == json_fields_.end()) {
    return None{};
  }
  return it->second;
}

auto SqlSchema::placed_column(std::span<const std::string> path) const
  -> Option<std::string const&> {
  auto it = placed_.find(std::vector<std::string>{path.begin(), path.end()});
  if (it == placed_.end()) {
    return None{};
  }
  return it->second;
}

auto make_catch_all_schema(std::span<ColumnDescription const> description,
                           CatchAllLayout const& layout,
                           ir::OptimizeFilter const& filter, bool json_fields)
  -> SqlSchema {
  auto result = SqlSchema{};
  for (auto const& column : description) {
    auto const* placed = layout.find(column.name);
    if (not placed or placed->json) {
      continue;
    }
    auto path = std::vector<std::string>{};
    for (auto const& segment : placed->path) {
      path.push_back(segment.id.name);
    }
    // A column that shares its field with another one, or that `SELECT *`
    // may leave out, does not hold the field in every row.
    auto origin = layout.origin(path);
    if (not origin or origin->column != column.name
        or not origin->path.empty()) {
      continue;
    }
    if (path.size() == 1 and path.front() == column.name) {
      result.add_column(column.name, column.type);
    } else {
      result.add_placed_column(column.name, column.type, std::move(path));
    }
  }
  if (not json_fields) {
    return result;
  }
  auto collector = FieldPathCollector{};
  for (auto expr : filter) {
    collector.visit(expr);
  }
  for (auto& path : collector.paths) {
    auto origin = layout.origin(path);
    if (not origin or not origin->json) {
      continue;
    }
    auto const column = std::ranges::find(description, origin->column,
                                          &ColumnDescription::name);
    TENZIR_ASSERT(column != description.end());
    if (is_untyped_json(column->type)) {
      result.add_json_field(std::move(path), origin->column, origin->path);
    }
  }
  return result;
}

auto SqlSchema::is_generated(std::string_view name) const -> bool {
  return generated_.contains(std::string{name});
}

auto SqlSchema::columns() const -> std::span<const std::string> {
  return columns_;
}

auto SqlSchema::model() const -> pushdown::ColumnModel const& {
  return model_;
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

auto supports_aggregate_functions(ir::OptimizeAggregation const& aggregation)
  -> bool {
  return std::ranges::all_of(
    aggregation.columns, [](ir::OptimizeAggregation::Column const& column) {
      if (not column.function) {
        return true;
      }
      auto const& function = *column.function;
      return function == "count"
             or ((function == "min" or function == "max") and column.input);
    });
}

auto make_aggregate_query(ir::OptimizeAggregation const& aggregation,
                          SqlSchema const& schema, std::string_view table,
                          std::span<const std::string> where, SqlTarget target)
  -> Option<std::string> {
  if (aggregation.columns.empty()
      or not supports_aggregate_functions(aggregation)) {
    return None{};
  }
  auto const renderer = ClickHouseRenderer{target};
  auto selection = std::vector<std::string>{};
  auto groups = std::vector<std::string>{};
  for (auto i = size_t{0}; i < aggregation.columns.size(); ++i) {
    auto const& column = aggregation.columns[i];
    auto alias = pushdown::aggregation_column(i);
    // ClickHouse resolves a name to an alias before a column, so an alias
    // that names a column would change what the query reads.
    if (std::ranges::contains(schema.columns(), alias)) {
      return None{};
    }
    auto input = Option<pushdown::FieldValue>{};
    auto sql = Option<std::string>{};
    if (column.input) {
      TRY(input, pushdown::translate_field(*column.input, schema.model()));
      TRY(sql, renderer.render(input->expr));
    }
    auto value = std::string{};
    if (not column.function) {
      TENZIR_ASSERT(input);
      if (is<pushdown::FloatType>(input->column.type)) {
        return None{};
      }
      groups.push_back(*sql);
      value = std::move(*sql);
    } else if (*column.function == "count") {
      value = fmt::format("toInt64(count({}))", sql ? *sql : std::string{});
    } else {
      TENZIR_ASSERT(input);
      if (not(is<pushdown::IntType>(input->column.type)
              or is<pushdown::TimeType>(input->column.type))) {
        return None{};
      }
      value = fmt::format("{}OrNull({})", *column.function, *sql);
    }
    selection.push_back(
      fmt::format("{} AS {}", value, quote_sql_identifier(alias)));
  }
  auto query
    = fmt::format("SELECT {} FROM {}", fmt::join(selection, ", "), table);
  if (not where.empty()) {
    fmt::format_to(std::back_inserter(query), " WHERE {}",
                   fmt::join(where, " AND "));
  }
  if (not groups.empty()) {
    fmt::format_to(std::back_inserter(query), " GROUP BY {}",
                   fmt::join(groups, ", "));
  }
  return query;
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
