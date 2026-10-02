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
#include "tenzir/detail/escapers.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/string.hpp"

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

auto ClickHouseRenderer::render_call(pushdown::Call const& x,
                                     std::span<Fragment const> args) const
  -> Option<Fragment> {
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
