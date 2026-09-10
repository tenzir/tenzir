//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/sql_pushdown.hpp"

#include "clickhouse/block_to_table_slice.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <string>
#include <string_view>

namespace tenzir::plugins::clickhouse {

namespace {

auto make_schema() -> SqlSchema {
  auto schema = SqlSchema{};
  schema.add_column("id", "UInt64");
  schema.add_column("x", "Int64");
  schema.add_column("y", "String");
  schema.add_column("flag", "Bool");
  schema.add_column("n", "Nullable(Int64)");
  schema.add_column("s", "LowCardinality(Nullable(String))");
  schema.add_column("f", "Float32");
  schema.add_column("status", "Enum8('low' = 1, 'high' = 2, 'a\\\\b' = 4, "
                              "'x, y' = 5, 'tab\\there' = 6, 'eq=' = 9)");
  schema.add_column("stamp", "DateTime64(3)");
  schema.add_column("meta", "Tuple(\n    source String,\n    level "
                            "Nullable(Int64),\n    pos Tuple(Int64, Int64))");
  schema.add_column("weird.name", "Int64");
  schema.add_column("gen", "Int64", true);
  schema.add_column("i32", "Int32");
  schema.add_column("u32", "Nullable(UInt32)");
  schema.add_column("u8", "UInt8");
  schema.add_column("f64", "Float64");
  schema.add_column("d", "Date");
  schema.add_column("d32", "Date32");
  schema.add_column("dt", "DateTime('Asia/Tokyo')");
  schema.add_column("dt2", "Nullable(DateTime64(2))");
  schema.add_column("dt3", "DateTime64(3, 'UTC')");
  schema.add_column("dt9", "DateTime64(9)");
  schema.add_column("ip4", "IPv4");
  schema.add_column("ip6", "Nullable(IPv6)");
  schema.add_column("uid", "UUID");
  schema.add_column("fs", "FixedString(4)");
  schema.add_column("dec", "Decimal(10, 2)");
  // A tuple with an element that does not decode is dropped locally as a
  // whole, so none of its elements is addressable.
  schema.add_column("half", "Tuple(ok String, bad Map(String, Int64))");
  schema.add_column("bad", "Map(String, Int64)");
  return schema;
}

auto parse(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ctx));
  return std::move(*expr);
}

auto translate(std::string_view source) -> Option<std::string> {
  return translate_predicate(parse(source), make_schema());
}

/// Adapts `source` to the schema and returns whether the left-hand side of the
/// resulting binary expression is a call of `ip`.
auto adapts_to_ip_call(std::string_view source) -> bool {
  auto expr = parse(source);
  adapt_to_schema(expr, make_schema());
  auto const* binary = try_as<ast::binary_expr>(expr);
  if (not binary) {
    return false;
  }
  auto const* call = try_as<ast::function_call>(binary->left);
  return call and call->fn.path.size() == 1
         and call->fn.path.front().name == "ip" and call->fn.ref.resolved()
         and call->args.size() == 1;
}

/// Adapts `source` and returns its exact translation.
auto adapt_translate(std::string_view source) -> Option<std::string> {
  auto expr = parse(source);
  adapt_to_schema(expr, make_schema());
  return translate_predicate(expr, make_schema());
}

/// Adapts `source` and returns its prefilter.
auto prefilter(std::string_view source) -> Option<std::string> {
  auto expr = parse(source);
  adapt_to_schema(expr, make_schema());
  return translate_prefilter(expr, make_schema());
}

} // namespace

TEST("comparisons against non-nullable columns") {
  CHECK_EQUAL(translate("x > 0"), std::string{"`x` > 0"});
  CHECK_EQUAL(translate("0 < x"), std::string{"`x` > 0"});
  CHECK_EQUAL(translate("x == -3"), std::string{"`x` = -3"});
  CHECK_EQUAL(translate("x != 1.5"), std::string{"`x` != 1.5"});
  CHECK_EQUAL(translate("f <= -0.5"), std::string{"`f` <= -0.5"});
  CHECK_EQUAL(translate("x in [-1, 2]"), std::string{"`x` IN (-1, 2)"});
  CHECK_EQUAL(translate("id >= 18446744073709551615"),
              std::string{"`id` >= 18446744073709551615"});
  CHECK_EQUAL(translate("y == \"foo\""), std::string{"`y` = 'foo'"});
  CHECK_EQUAL(translate("flag == true"), std::string{"`flag` = true"});
  CHECK_EQUAL(translate("flag"), std::string{"`flag`"});
  CHECK_EQUAL(translate("f < 2.5"), std::string{"`f` < 2.5"});
  CHECK_EQUAL(translate("f == 1"), std::string{"`f` = 1"});
  CHECK_EQUAL(translate("x in [1.0, 2.5]"), std::string{"`x` IN (1., 2.5)"});
}

TEST("in requires a homogeneous list") {
  // TQL fixes the element type from the first element and turns clashing
  // elements into `null`, so `x in [1, 2.0]` never matches `2`.
  CHECK(not translate("x in [1, 2.0]"));
  CHECK(not translate("x in [1.0, 2]"));
  CHECK(not translate("f in [1, 2.0]"));
  CHECK(not translate("f in [2.0, 1]"));
  // `18446744073709551615` is a `uint64` literal, `1` is `int64`.
  CHECK(not translate("id in [1, 18446744073709551615]"));
  CHECK_EQUAL(translate("id in [18446744073709551615]"),
              std::string{"`id` IN (18446744073709551615)"});
  CHECK(not translate("y in [\"a\", 1]"));
  CHECK(not translate("flag in [true, 1]"));
  // Negative literals fold to the same type as positive ones.
  CHECK_EQUAL(translate("x in [-1, 2]"), std::string{"`x` IN (-1, 2)"});
  CHECK_EQUAL(translate("f in [-1.5, 2.0]"), std::string{"`f` IN (-1.5, 2.)"});
}

TEST("mixed integer and floating-point comparisons stop at 2^53") {
  // TQL compares mixed operands in double, ClickHouse exactly. Below 2^53 the
  // two agree; at and beyond it they may not.
  CHECK_EQUAL(translate("x == 4503599627370496.0"),
              std::string{"`x` = 4503599627370496."});
  CHECK_EQUAL(translate("x > -9007199254740991.0"),
              std::string{"`x` > -9007199254740991."});
  CHECK(not translate("x == 9007199254740992.0"));
  CHECK(not translate("x < -9007199254740992.0"));
  CHECK(not translate("x in [9007199254740992.0]"));
  // An integer literal against a floating column only needs to be exactly
  // representable, which holds for 2^53 and for larger powers of two.
  CHECK_EQUAL(translate("f == 9007199254740992"),
              std::string{"`f` = 9007199254740992"});
  CHECK_EQUAL(translate("f in [1, 1152921504606846976]"),
              std::string{"`f` IN (1, 1152921504606846976)"});
  CHECK(not translate("f >= 9007199254740993"));
  CHECK(not translate("f in [1, 9007199254740993]"));
  CHECK(not translate("f == 18446744073709551615"));
  CHECK(not translate("f == -9223372036854775807"));
  // Same-kind comparisons have no bound.
  CHECK_EQUAL(translate("id >= 18446744073709551615"),
              std::string{"`id` >= 18446744073709551615"});
  CHECK_EQUAL(translate("x in [9007199254740993, -9007199254740993]"),
              std::string{"`x` IN (9007199254740993, -9007199254740993)"});
  CHECK_EQUAL(translate("f > 1e300"), std::string{"`f` > 1e+300"});
}

TEST("nullable columns get a null guard on equality") {
  CHECK_EQUAL(translate("n == 1"),
              std::string{"(`n` IS NOT NULL AND `n` = 1)"});
  CHECK_EQUAL(translate("n != 1"), std::string{"(`n` IS NULL OR `n` != 1)"});
  CHECK_EQUAL(translate("n > 1"), std::string{"`n` > 1"});
  CHECK_EQUAL(translate("n == null"), std::string{"`n` IS NULL"});
  CHECK_EQUAL(translate("n != null"), std::string{"`n` IS NOT NULL"});
  CHECK_EQUAL(translate("s == \"a\""),
              std::string{"(`s` IS NOT NULL AND `s` = 'a')"});
  CHECK_EQUAL(translate("n in [1, 2]"),
              std::string{"(`n` IS NOT NULL AND `n` IN (1, 2))"});
}

TEST("boolean combinators") {
  CHECK_EQUAL(translate("x > 0 or y == \"foo\" and x in [1, 2, 3]"),
              std::string{"(`x` > 0 OR (`y` = 'foo' AND `x` IN (1, 2, 3)))"});
  CHECK_EQUAL(translate("not (x == 1)"), std::string{"NOT `x` = 1"});
  CHECK_EQUAL(translate("not (n == 1)"),
              std::string{"NOT (`n` IS NOT NULL AND `n` = 1)"});
  // One untranslatable side taints the whole disjunction.
  CHECK(not translate("x > 0 or y.length_chars() > 3"));
}

TEST("nested tuple elements") {
  CHECK_EQUAL(translate("meta.source == \"a\""),
              std::string{"`meta`.`source` = 'a'"});
  CHECK_EQUAL(translate("meta.level > 2"), std::string{"`meta`.`level` > 2"});
  CHECK_EQUAL(translate("meta.level == 2"),
              std::string{"(`meta`.`level` IS NOT NULL AND `meta`.`level` = "
                          "2)"});
  // Unnamed tuple elements and the tuple itself are not comparable.
  CHECK(not translate("meta.pos == 1"));
  CHECK(not translate("meta == 1"));
  // A top-level column whose name contains a dot is not a nested path.
  CHECK(not translate("weird.name == 1"));
}

TEST("string ordering compares bytes") {
  CHECK_EQUAL(translate("y < \"b\""), std::string{"`y` < 'b'"});
  CHECK_EQUAL(translate("\"b\" <= y"), std::string{"`y` >= 'b'"});
  CHECK_EQUAL(translate("s > \"\""), std::string{"`s` > ''"});
}

TEST("string functions") {
  CHECK_EQUAL(translate("y.starts_with(\"f\")"),
              std::string{"startsWith(`y`, 'f')"});
  CHECK_EQUAL(translate("ends_with(s, \"it's\")"),
              std::string{"endsWith(`s`, 'it\\'s')"});
  CHECK_EQUAL(translate("\"oo\" in y"), std::string{"position(`y`, 'oo') > 0"});
  CHECK_EQUAL(translate("y.length_bytes() > 3"),
              std::string{"length(`y`) > 3"});
  CHECK_EQUAL(translate("fs.length_bytes() == 4"),
              std::string{"length(`fs`) = 4"});
  CHECK_EQUAL(translate("length_bytes(s) in [1, 2]"),
              std::string{"(length(`s`) IS NOT NULL AND length(`s`) IN (1, "
                          "2))"});
  // Case folding, non-string subjects, and other functions stay local.
  CHECK(not translate("y.starts_with(\"f\", ignore_case=true)"));
  CHECK(not translate("status.starts_with(\"h\")"));
  CHECK(not translate("y.starts_with(s)"));
  CHECK(not translate("y.length_chars() > 3"));
  CHECK(not translate("x.length_bytes() > 3"));
  CHECK(not translate("y.to_upper() == \"A\""));
}

TEST("enum, uuid, and fixed string equality") {
  CHECK_EQUAL(translate("status == \"high\""),
              std::string{"`status` = 'high'"});
  CHECK_EQUAL(translate("status in [\"low\", \"nope\", \"high\"]"),
              std::string{"`status` IN ('low', 'high')"});
  // The decoder takes a label's rendering between the quotes verbatim, so
  // TQL sees `a\\b` and `tab\there` with their backslashes, and the SQL
  // literal is that rendering quoted again.
  CHECK_EQUAL(translate("status == \"a\\\\\\\\b\""),
              std::string{"`status` = 'a\\\\b'"});
  CHECK_EQUAL(translate("status == \"a\\\\b\""), std::string{"false"});
  CHECK_EQUAL(translate("status == \"tab\\\\there\""),
              std::string{"`status` = 'tab\\there'"});
  CHECK_EQUAL(translate("status == \"tab\\there\""), std::string{"false"});
  CHECK_EQUAL(translate("status == \"x, y\""),
              std::string{"`status` = 'x, y'"});
  CHECK_EQUAL(translate("status == \"eq=\""), std::string{"`status` = 'eq='"});
  // A label with a quote defeats the decoder's type parser, so the column is
  // not readable at all and nothing on it is pushed.
  auto quoted = SqlSchema{};
  quoted.add_column("e", "Enum8('it\\'s' = 1)");
  CHECK(not translate_predicate(parse("e == \"it's\""), quoted));
  // An unknown name never matches, and ClickHouse might reject it.
  CHECK_EQUAL(translate("status == \"nope\""), std::string{"false"});
  CHECK_EQUAL(translate("status != \"nope\""), std::string{"true"});
  CHECK_EQUAL(translate("status in [\"nope\"]"), std::string{"false"});
  // TQL orders enums by name, ClickHouse by value.
  CHECK(not translate("status < \"low\""));
  // UUIDs match in their canonical form only.
  CHECK_EQUAL(translate("uid == \"61f0c404-5cb3-11e7-907b-a6006ad3dba0\""),
              std::string{"`uid` = '61f0c404-5cb3-11e7-907b-a6006ad3dba0'"});
  CHECK_EQUAL(translate("uid == \"61F0C404-5CB3-11E7-907B-A6006AD3DBA0\""),
              std::string{"false"});
  CHECK_EQUAL(translate("uid != \"not a uuid\""), std::string{"true"});
  CHECK(not translate("uid < \"61f0c404-5cb3-11e7-907b-a6006ad3dba0\""));
  // A fixed string keeps its zero padding in TQL, so only a literal of the
  // full length can match; ClickHouse would pad a shorter one.
  CHECK_EQUAL(translate("fs == \"abcd\""), std::string{"`fs` = 'abcd'"});
  CHECK_EQUAL(translate("fs == \"ab\\u0000\\u0000\""),
              std::string{"`fs` = 'ab\\x00\\x00'"});
  CHECK_EQUAL(translate("fs == \"ab\""), std::string{"false"});
  CHECK_EQUAL(translate("fs != \"ab\""), std::string{"true"});
  CHECK_EQUAL(translate("fs == \"abcde\""), std::string{"false"});
  CHECK_EQUAL(translate("fs in [\"a\", \"abcd\", \"abcde\"]"),
              std::string{"`fs` IN ('abcd')"});
  CHECK_EQUAL(translate("fs in [\"a\"]"), std::string{"false"});
  CHECK(not translate("fs < \"b\""));
  // Decimals are text in TQL and numbers in ClickHouse.
  CHECK(not translate("dec == \"1.50\""));
  CHECK(not translate("dec == 1.5"));
}

TEST("time literals are rendered in the column's type") {
  // Each type gets a literal that denotes the same instant in any time zone.
  CHECK_EQUAL(translate("d == 2024-01-01"),
              std::string{"`d` = toDate('2024-01-01')"});
  CHECK_EQUAL(translate("d32 < 2024-01-01"),
              std::string{"(`d32` <= toDate32('2262-04-11') AND `d32` < "
                          "toDate32('2024-01-01'))"});
  CHECK_EQUAL(translate("dt >= 2024-01-01T12:34:56"),
              std::string{"`dt` >= toDateTime('2024-01-01 12:34:56', 'UTC')"});
  CHECK_EQUAL(translate("dt9 == 2024-01-01T12:34:56.789"),
              std::string{"`dt9` = fromUnixTimestamp64Nano("
                          "1704112496789000000)"});
  CHECK_EQUAL(translate("stamp == 2024-01-01T12:34:56.789"),
              std::string{"`stamp` = fromUnixTimestamp64Milli(1704112496789)"});
  CHECK_EQUAL(translate("dt2 == 2024-01-01T12:34:56.78"),
              std::string{"(`dt2` IS NOT NULL AND `dt2` = "
                          "CAST(fromUnixTimestamp64Nano(1704112496780000000), "
                          "'DateTime64(2)'))"});
  CHECK_EQUAL(translate("dt3 in [2024-01-01, 2024-01-01T00:00:00.001]"),
              std::string{"`dt3` IN (fromUnixTimestamp64Milli(1704067200000), "
                          "fromUnixTimestamp64Milli(1704067200001))"});
  // Literals fold before translation.
  CHECK_EQUAL(translate("d == 2024-01-01 + 1d"),
              std::string{"`d` = toDate('2024-01-02')"});
  CHECK_EQUAL(translate("2024-01-01 - 1s > dt"),
              std::string{"`dt` < toDateTime('2023-12-31 23:59:59', 'UTC')"});
  // Non-time literals and unsupported types stay local.
  CHECK(not translate("d == \"2024-01-01\""));
  CHECK(not translate("d == 1704067200"));
  CHECK(not translate("y == 2024-01-01"));
  CHECK(not translate("stamp > 1h"));
}

TEST("time literals between ticks and outside the range resolve statically") {
  // A sub-day instant never equals a `Date`; ordering moves to the nearest
  // day: `d < t` holds exactly when `d <= floor(t)`, and `d >= t` exactly
  // when `d > floor(t)`.
  CHECK_EQUAL(translate("d == 2024-01-01T00:00:01"), std::string{"false"});
  CHECK_EQUAL(translate("d != 2024-01-01T00:00:01"), std::string{"true"});
  CHECK_EQUAL(translate("d < 2024-01-01T00:00:01"),
              std::string{"`d` <= toDate('2024-01-01')"});
  CHECK_EQUAL(translate("d <= 2024-01-01T00:00:01"),
              std::string{"`d` <= toDate('2024-01-01')"});
  CHECK_EQUAL(translate("d > 2024-01-01T00:00:01"),
              std::string{"`d` > toDate('2024-01-01')"});
  CHECK_EQUAL(translate("d >= 2024-01-01T00:00:01"),
              std::string{"`d` > toDate('2024-01-01')"});
  CHECK_EQUAL(translate("dt < 2024-01-01T12:34:56.5"),
              std::string{"`dt` <= toDateTime('2024-01-01 12:34:56', 'UTC')"});
  CHECK_EQUAL(translate("stamp == 2024-01-01T12:34:56.7891"),
              std::string{"false"});
  CHECK_EQUAL(translate("stamp in [2024-01-01T12:34:56.7891]"),
              std::string{"false"});
  // Instants outside the type's range compare against its edge, which keeps
  // `null` rows `null`.
  CHECK_EQUAL(translate("d < 1969-12-31"),
              std::string{"`d` < toDate('1970-01-01')"});
  CHECK_EQUAL(translate("d >= 1969-12-31"),
              std::string{"`d` >= toDate('1970-01-01')"});
  CHECK_EQUAL(translate("d <= 2150-01-01"),
              std::string{"`d` <= toDate('2149-06-06')"});
  CHECK_EQUAL(translate("d > 2150-01-01"),
              std::string{"`d` > toDate('2149-06-06')"});
  CHECK_EQUAL(translate("d == 2150-01-01"), std::string{"false"});
  CHECK_EQUAL(translate("dt > 2107-01-01"),
              std::string{"`dt` > toDateTime('2106-02-07 06:28:15', 'UTC')"});
  CHECK_EQUAL(translate("dt in [1969-12-31, 2024-01-01]"),
              std::string{"`dt` IN (toDateTime('2024-01-01 00:00:00', "
                          "'UTC'))"});
  // `Date32` reaches past what TQL can decode, so ordering is guarded to
  // treat those rows as `null`. Equality needs no guard: no literal can hit
  // them.
  CHECK_EQUAL(translate("d32 == 2024-01-01"),
              std::string{"`d32` = toDate32('2024-01-01')"});
  CHECK_EQUAL(translate("d32 > 1900-01-01"),
              std::string{"(`d32` <= toDate32('2262-04-11') AND `d32` > "
                          "toDate32('1900-01-01'))"});
  CHECK_EQUAL(translate("d32 >= 1900-01-01 - 1d"),
              std::string{"(`d32` <= toDate32('2262-04-11') AND `d32` >= "
                          "toDate32('1900-01-01'))"});
  // Under `not`, `false` would keep the row where TQL's `null` drops it, so
  // the guard makes the comparison `NULL` instead.
  CHECK_EQUAL(translate("not (d32 > 1900-01-01)"),
              std::string{"NOT if(`d32` <= toDate32('2262-04-11'), `d32` > "
                          "toDate32('1900-01-01'), NULL)"});
  CHECK_EQUAL(translate("not (d32 > 1900-01-01 or x > 0)"),
              std::string{"NOT (if(`d32` <= toDate32('2262-04-11'), `d32` > "
                          "toDate32('1900-01-01'), NULL) OR `x` > 0)"});
  CHECK_EQUAL(translate("d32 > 1900-01-01 or x > 0"),
              std::string{"((`d32` <= toDate32('2262-04-11') AND `d32` > "
                          "toDate32('1900-01-01')) OR `x` > 0)"});
  CHECK_EQUAL(translate("not (not (d32 > 1900-01-01))"),
              std::string{"NOT NOT if(`d32` <= toDate32('2262-04-11'), `d32` "
                          "> toDate32('1900-01-01'), NULL)"});
  // So does `DateTime64` below nanosecond precision, on both ends.
  CHECK_EQUAL(translate("stamp > 2024-01-01"),
              std::string{"(`stamp` >= fromUnixTimestamp64Milli("
                          "-9223372036854) AND `stamp` <= "
                          "fromUnixTimestamp64Milli(9223372036854) AND "
                          "`stamp` > fromUnixTimestamp64Milli("
                          "1704067200000))"});
  CHECK_EQUAL(translate("dt9 > 2024-01-01"),
              std::string{"`dt9` > fromUnixTimestamp64Nano("
                          "1704067200000000000)"});
  // Null checks see the rows that TQL cannot decode as `null` as well.
  CHECK_EQUAL(translate("d32 == null"),
              std::string{"if(`d32` <= toDate32('2262-04-11'), `d32`, NULL) "
                          "IS NULL"});
  CHECK_EQUAL(translate("dt9 != null"), std::string{"`dt9` IS NOT NULL"});
  CHECK_EQUAL(translate("dt2 == null"),
              std::string{"if(`dt2` >= CAST(fromUnixTimestamp64Nano("
                          "-9223372036850000000), 'DateTime64(2)') AND `dt2` "
                          "<= CAST(fromUnixTimestamp64Nano("
                          "9223372036850000000), 'DateTime64(2)'), `dt2`, "
                          "NULL) IS NULL"});
}

TEST("ip literals are rendered in the column's family") {
  CHECK_EQUAL(translate("ip4 == 10.0.0.1"),
              std::string{"`ip4` = toIPv4('10.0.0.1')"});
  CHECK_EQUAL(translate("ip6 == 10.0.0.1"),
              std::string{"(`ip6` IS NOT NULL AND `ip6` = "
                          "toIPv6('10.0.0.1'))"});
  CHECK_EQUAL(translate("ip6 == ::1"),
              std::string{"(`ip6` IS NOT NULL AND `ip6` = toIPv6('::1'))"});
  CHECK_EQUAL(translate("ip6 < 2001:db8::"),
              std::string{"`ip6` < toIPv6('2001:db8::')"});
  CHECK_EQUAL(translate("ip4 in [10.0.0.1, ::1, 10.0.0.2]"),
              std::string{"`ip4` IN (toIPv4('10.0.0.1'), toIPv4('10.0.0.2'))"});
  CHECK_EQUAL(translate("ip6 in [10.0.0.1, ::1]"),
              std::string{
                "(`ip6` IS NOT NULL AND `ip6` IN (toIPv6('10.0.0.1'), "
                "toIPv6('::1')))"});
  // An IPv6 address against an `IPv4` column lies outside the mapped range.
  CHECK_EQUAL(translate("ip4 == ::1"), std::string{"false"});
  CHECK_EQUAL(translate("ip4 != ::1"), std::string{"true"});
  CHECK_EQUAL(translate("ip4 in [::1]"), std::string{"false"});
  CHECK_EQUAL(translate("ip4 > ::1"),
              std::string{"`ip4` >= toIPv4('0.0.0.0')"});
  CHECK_EQUAL(translate("ip4 <= ::1"),
              std::string{"`ip4` < toIPv4('0.0.0.0')"});
  CHECK_EQUAL(translate("ip4 < 2001:db8::"),
              std::string{"`ip4` <= toIPv4('255.255.255.255')"});
  CHECK_EQUAL(translate("ip4 >= 2001:db8::"),
              std::string{"`ip4` > toIPv4('255.255.255.255')"});
  CHECK(not translate("ip4 == \"10.0.0.1\""));
  CHECK(not translate("y == 10.0.0.1"));
}

TEST("subnet membership becomes a range") {
  CHECK_EQUAL(translate("ip4 in 10.0.0.0/8"),
              std::string{"`ip4` BETWEEN toIPv4('10.0.0.0') AND "
                          "toIPv4('10.255.255.255')"});
  CHECK_EQUAL(translate("ip4 in 10.1.2.3/30"),
              std::string{"`ip4` BETWEEN toIPv4('10.1.2.0') AND "
                          "toIPv4('10.1.2.3')"});
  CHECK_EQUAL(translate("ip6 in 2001:db8::/32"),
              std::string{"`ip6` BETWEEN toIPv6('2001:db8::') AND "
                          "toIPv6('2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')"});
  CHECK_EQUAL(translate("ip6 in 10.0.0.0/8"),
              std::string{"`ip6` BETWEEN toIPv6('10.0.0.0') AND "
                          "toIPv6('10.255.255.255')"});
  // Ranges that cover or miss the IPv4-mapped space entirely.
  CHECK_EQUAL(translate("ip4 in ::/0"),
              std::string{"`ip4` BETWEEN toIPv4('0.0.0.0') AND "
                          "toIPv4('255.255.255.255')"});
  CHECK_EQUAL(translate("ip4 in ::ffff:0:0/96"),
              std::string{"`ip4` BETWEEN toIPv4('0.0.0.0') AND "
                          "toIPv4('255.255.255.255')"});
  CHECK_EQUAL(translate("ip4 in 2001:db8::/32"),
              std::string{"`ip4` < toIPv4('0.0.0.0')"});
  CHECK(not translate("y in 10.0.0.0/8"));
  CHECK(not translate("ip4 in [10.0.0.0/8]"));
}

TEST("arithmetic on small integers and floats") {
  CHECK_EQUAL(translate("i32 + 1 > 5"), std::string{"(`i32` + 1) > 5"});
  CHECK_EQUAL(translate("1 + i32 > 5"), std::string{"(1 + `i32`) > 5"});
  CHECK_EQUAL(translate("i32 - -1 > 5"), std::string{"(`i32` - -1) > 5"});
  CHECK_EQUAL(translate("10 - i32 == 5"), std::string{"(10 - `i32`) = 5"});
  CHECK_EQUAL(translate("i32 * 1000 >= 5"), std::string{"(`i32` * 1000) >= 5"});
  CHECK_EQUAL(translate("u8 + 2147483647 == 5"),
              std::string{"(`u8` + 2147483647) = 5"});
  CHECK_EQUAL(translate("u32 - -1 == 5"),
              std::string{"((`u32` - -1) IS NOT NULL AND (`u32` - -1) = 5)"});
  CHECK_EQUAL(translate("5 - u32 == -1"),
              std::string{"((5 - `u32`) IS NOT NULL AND (5 - `u32`) = -1)"});
  // Division yields a double on both sides.
  CHECK_EQUAL(translate("x / 2 > 3"), std::string{"(`x` / 2) > 3"});
  CHECK_EQUAL(translate("id / 3.5 > 3"), std::string{"(`id` / 3.5) > 3"});
  CHECK_EQUAL(translate("f64 * 2 > 3"), std::string{"(`f64` * 2) > 3"});
  CHECK_EQUAL(translate("f - 0.5 <= 3"), std::string{"(`f` - 0.5) <= 3"});
  CHECK_EQUAL(translate("x + 0.5 <= 3"), std::string{"(`x` + 0.5) <= 3"});
  // A `double` literal with an integral value keeps a fraction, so ClickHouse
  // computes in `Float64` like TQL does, instead of exact integer arithmetic.
  CHECK_EQUAL(translate("id + 0.0 == 9007199254740992"),
              std::string{"(`id` + 0.) = 9007199254740992"});
  CHECK_EQUAL(translate("x * 2.0 > 3"), std::string{"(`x` * 2.) > 3"});
  CHECK_EQUAL(translate("x / 2.0 > 3"), std::string{"(`x` / 2.) > 3"});
  CHECK_EQUAL(translate("1e16 - x > 3"), std::string{"(1e+16 - `x`) > 3"});
  CHECK_EQUAL(translate("(x + 1.5) == 1e300"),
              std::string{"(`x` + 1.5) = 1e+300"});
  // The result of integer arithmetic is a 64-bit integer, so a literal
  // beyond 2^53 still compares exactly.
  CHECK_EQUAL(translate("i32 + 1 == 9007199254740993"),
              std::string{"(`i32` + 1) = 9007199254740993"});
  CHECK(not translate("i32 + 1 == 9007199254740992.0"));
  // Overflow is possible on 64-bit columns, with large literals, and with
  // two columns; unsigned results must not go below zero; division by zero
  // yields `null`.
  CHECK(not translate("x + 1 > 5"));
  CHECK(not translate("id - 1 > 5"));
  CHECK(not translate("i32 + 2147483648 > 5"));
  CHECK(not translate("i32 * -2147483648 > 5"));
  CHECK(not translate("i32 + u8 > 5"));
  CHECK(not translate("u32 - 1 == 5"));
  CHECK(not translate("u32 + -1 == 5"));
  CHECK(not translate("u32 * -1 == 5"));
  CHECK(not translate("x / 0 > 3"));
  CHECK(not translate("x / 0.0 > 3"));
  CHECK(not translate("1 / x > 3"));
  CHECK(not translate("i32 + 1 + 1 > 5"));
  CHECK(not translate("y + \"a\" == \"ba\""));
  CHECK(not translate("stamp + 1h > 2024-01-01"));
}

TEST("literal expressions fold") {
  CHECK_EQUAL(translate("x > 1024 * 1024"), std::string{"`x` > 1048576"});
  CHECK_EQUAL(translate("x > -(2 + 3)"), std::string{"`x` > -5"});
  CHECK_EQUAL(translate("y == \"a\" + \"b\""), std::string{"`y` = 'ab'"});
  CHECK_EQUAL(translate("x in [1 + 1, 3 - 1]"), std::string{"`x` IN (2, 2)"});
  // Division folds to a double, so this list mixes types.
  CHECK(not translate("x in [1 + 1, 4 / 2]"));
  // An overflowing or failing expression stays local, where it warns.
  CHECK(not translate("x > 9223372036854775807 + 1"));
  CHECK(not translate("x > 1 / 0"));
  CHECK(not translate("stamp > now() - 1h"));
}

TEST("comparisons between two columns") {
  CHECK_EQUAL(translate("x < id"), std::string{"`x` < `id`"});
  CHECK_EQUAL(translate("x == id"), std::string{"`x` = `id`"});
  CHECK_EQUAL(translate("y != meta.source"),
              std::string{"`y` != `meta`.`source`"});
  CHECK_EQUAL(translate("f64 >= f"), std::string{"`f64` >= `f`"});
  CHECK_EQUAL(translate("ip4 == ip6"),
              std::string{"(`ip6` IS NOT NULL AND `ip4` = `ip6`)"});
  CHECK_EQUAL(translate("dt9 < stamp + 0"), Option<std::string>{});
  CHECK_EQUAL(translate("d < d"), std::string{"`d` < `d`"});
  CHECK_EQUAL(translate("dt9 < dt9"), std::string{"`dt9` < `dt9`"});
  CHECK_EQUAL(translate("i32 + 1 < x"), std::string{"(`i32` + 1) < `x`"});
  // TQL treats `null` as a value for `==`, `!=`, `<=`, and `>=`.
  CHECK_EQUAL(translate("n == x"),
              std::string{"(`n` IS NOT NULL AND `n` = `x`)"});
  CHECK_EQUAL(translate("n != x"),
              std::string{"NOT (`n` IS NOT NULL AND `n` = `x`)"});
  CHECK_EQUAL(translate("n == u32"),
              std::string{"((`n` IS NULL AND `u32` IS NULL) OR (`n` IS NOT "
                          "NULL AND `u32` IS NOT NULL AND `n` = `u32`))"});
  CHECK_EQUAL(translate("n <= u32"),
              std::string{"((`n` IS NULL AND `u32` IS NULL) OR `n` <= `u32`)"});
  CHECK_EQUAL(translate("n < u32"), std::string{"`n` < `u32`"});
  CHECK_EQUAL(translate("n >= x"), std::string{"`n` >= `x`"});
  // Temporal columns must share their storage type, and rows that TQL
  // decodes to `null` must compare as `null`.
  CHECK_EQUAL(
    translate("stamp == dt3"),
    std::string{
      "((if(`stamp` >= fromUnixTimestamp64Milli(-9223372036854) AND `stamp` "
      "<= fromUnixTimestamp64Milli(9223372036854), `stamp`, NULL) IS NULL AND "
      "if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
      "fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL) IS NULL) OR "
      "(if(`stamp` >= fromUnixTimestamp64Milli(-9223372036854) AND `stamp` <= "
      "fromUnixTimestamp64Milli(9223372036854), `stamp`, NULL) IS NOT NULL "
      "AND if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
      "fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL) IS NOT NULL AND "
      "if(`stamp` >= fromUnixTimestamp64Milli(-9223372036854) AND `stamp` <= "
      "fromUnixTimestamp64Milli(9223372036854), `stamp`, NULL) = if(`dt3` >= "
      "fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
      "fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL)))"});
  CHECK(not translate("d < d32"));
  CHECK(not translate("dt < stamp"));
  CHECK(not translate("stamp < dt9"));
  // Mixed kinds stay local.
  CHECK(not translate("x < f"));
  CHECK(not translate("y == status"));
  CHECK(not translate("y == fs"));
  CHECK(not translate("flag < flag"));
  CHECK(not translate("uid < uid"));
  CHECK(not translate("status == status"));
  CHECK(not translate("d == dt"));
}

TEST("ip literals against string columns become text") {
  // A `String` column holding addresses compares against the canonical text.
  CHECK_EQUAL(adapt_translate("y == 1.1.1.1"), std::string{"`y` = '1.1.1.1'"});
  CHECK_EQUAL(adapt_translate("1.1.1.1 != s"),
              std::string{"(`s` IS NULL OR `s` != '1.1.1.1')"});
  CHECK_EQUAL(adapt_translate("y == 2001:DB8::1"),
              std::string{"`y` = '2001:db8::1'"});
  CHECK_EQUAL(adapt_translate("y == ::ffff:1.1.1.1"),
              std::string{"`y` = '1.1.1.1'"});
  CHECK_EQUAL(adapt_translate("meta.source in [1.1.1.1, ::1]"),
              std::string{"`meta`.`source` IN ('1.1.1.1', '::1')"});
  CHECK_EQUAL(adapt_translate("not (y == 1.1.1.1 or x > 0)"),
              std::string{"NOT (`y` = '1.1.1.1' OR `x` > 0)"});
  // Membership in a subnet parses the column instead, which has no exact
  // translation.
  CHECK(adapts_to_ip_call("y in 10.0.0.0/8"));
  CHECK(not adapt_translate("y in 10.0.0.0/8"));
  // Ordering, mixed lists, and other column types are left alone.
  CHECK(not adapts_to_ip_call("y < 1.1.1.1"));
  CHECK(not adapt_translate("y < 1.1.1.1"));
  CHECK(not adapt_translate("y in [1.1.1.1, \"a\"]"));
  CHECK_EQUAL(adapt_translate("ip4 == 1.1.1.1"),
              std::string{"`ip4` = toIPv4('1.1.1.1')"});
  CHECK(not adapt_translate("fs == 1.1.1.1"));
  CHECK(not adapt_translate("x == 1.1.1.1"));
  CHECK(not adapt_translate("y.ip() == 1.1.1.1"));
  CHECK(not adapts_to_ip_call("ip4 in 10.0.0.0/8"));
  CHECK_EQUAL(adapt_translate("ip4 in 10.0.0.0/8"),
              std::string{"`ip4` BETWEEN toIPv4('10.0.0.0') AND "
                          "toIPv4('10.255.255.255')"});
}

TEST("parsed ip columns get a prefilter") {
  // The exact predicate stays local; ClickHouse drops what it can rule out and
  // keeps every string it cannot parse.
  CHECK_EQUAL(prefilter("y in 10.0.0.0/8"),
              std::string{"(toIPv6OrNull(`y`) IS NULL OR toIPv6OrNull(`y`) "
                          "BETWEEN toIPv6('10.0.0.0') AND "
                          "toIPv6('10.255.255.255'))"});
  CHECK_EQUAL(prefilter("s.ip() in 10.0.0.0/8"),
              std::string{"(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) "
                          "BETWEEN toIPv6('10.0.0.0') AND "
                          "toIPv6('10.255.255.255'))"});
  CHECK_EQUAL(prefilter("y.ip() == 1.1.1.1"),
              std::string{"(toIPv6OrNull(`y`) IS NULL OR toIPv6OrNull(`y`) = "
                          "toIPv6('1.1.1.1'))"});
  CHECK_EQUAL(prefilter("::1 > y.ip()"),
              std::string{"(toIPv6OrNull(`y`) IS NULL OR toIPv6OrNull(`y`) < "
                          "toIPv6('::1'))"});
  CHECK_EQUAL(prefilter("y.ip() in [1.1.1.1, ::1]"),
              std::string{"(toIPv6OrNull(`y`) IS NULL OR toIPv6OrNull(`y`) IN "
                          "(toIPv6('1.1.1.1'), toIPv6('::1')))"});
  // Prefilters compose through `and` and `or`.
  CHECK_EQUAL(prefilter("y in 10.0.0.0/8 or x > 0"),
              std::string{"((toIPv6OrNull(`y`) IS NULL OR toIPv6OrNull(`y`) "
                          "BETWEEN toIPv6('10.0.0.0') AND "
                          "toIPv6('10.255.255.255')) OR `x` > 0)"});
  CHECK_EQUAL(prefilter("(y in 10.0.0.0/8 and y.length_chars() > 3) or x > 0"),
              std::string{"((toIPv6OrNull(`y`) IS NULL OR toIPv6OrNull(`y`) "
                          "BETWEEN toIPv6('10.0.0.0') AND "
                          "toIPv6('10.255.255.255')) OR `x` > 0)"});
  CHECK_EQUAL(prefilter("y.length_chars() > 3 and x > 0"),
              std::string{"`x` > 0"});
  // An exact translation is its own prefilter.
  CHECK_EQUAL(prefilter("x > 0"), std::string{"`x` > 0"});
  // `!=` is `true` for a string TQL cannot parse, which ClickHouse might;
  // `not` has no superset; unrelated predicates have none either.
  CHECK(not prefilter("y.ip() != 1.1.1.1"));
  CHECK(not prefilter("not (y in 10.0.0.0/8)"));
  CHECK(not prefilter("y in 10.0.0.0/8 or y.length_chars() > 3"));
  CHECK(not prefilter("y.length_chars() > 3"));
  CHECK(not prefilter("ip4.ip() == 1.1.1.1"));
  CHECK(not prefilter("y.ip() == \"1.1.1.1\""));
}

TEST("predicates without an exact translation stay local") {
  // Type mismatches yield null in TQL but fail in ClickHouse.
  CHECK(not translate("x == \"foo\""));
  CHECK(not translate("y == 1"));
  CHECK(not translate("flag == 1"));
  CHECK(not translate("flag < true"));
  // Generated columns are not part of `SELECT *` by default.
  CHECK(not translate("gen == 1"));
  CHECK(not translate("gen > 1 and x > 1"));
  // Columns that the operator cannot decode are not in the local result, and
  // neither are the elements of a tuple that contains one.
  CHECK(not translate("bad == 1"));
  CHECK(not translate("half.ok == \"x\""));
  CHECK(not translate("half.ok.starts_with(\"x\")"));
  // Unknown columns, `this`, and unknown functions.
  CHECK(not translate("nope == 1"));
  CHECK(not translate("this == 1"));
  CHECK(not translate("x.abs() > 1"));
  // Lists with nulls or non-constants.
  CHECK(not translate("x in [1, null]"));
  CHECK(not translate("x in [id]"));
  CHECK(not translate("x in []"));
  // Two literals.
  CHECK(not translate("1 < 2"));
}

TEST("conjunctions are split into pushed and remaining parts") {
  auto filter = ir::OptimizeFilter{};
  filter.push_back(parse("x > 0 and y.to_upper() == \"F\" and n == null"));
  filter.push_back(parse("flag"));
  filter.push_back(parse("y == 1.1.1.1 and y in 10.0.0.0/8"));
  auto split = split_filter_for_sql(std::move(filter), make_schema());
  REQUIRE_EQUAL(split.pushed.size(), size_t{5});
  CHECK_EQUAL(split.pushed[0], std::string{"`x` > 0"});
  CHECK_EQUAL(split.pushed[1], std::string{"`n` IS NULL"});
  CHECK_EQUAL(split.pushed[2], std::string{"`flag`"});
  // The adapted equality is exact; the subnet test stays local behind its
  // prefilter.
  CHECK_EQUAL(split.pushed[3], std::string{"`y` = '1.1.1.1'"});
  CHECK_EQUAL(split.pushed[4],
              std::string{"(toIPv6OrNull(`y`) IS NULL OR toIPv6OrNull(`y`) "
                          "BETWEEN toIPv6('10.0.0.0') AND "
                          "toIPv6('10.255.255.255'))"});
  REQUIRE_EQUAL(split.remaining.size(), size_t{2});
  CHECK(is<ast::binary_expr>(split.remaining[0]));
  auto const* membership = try_as<ast::binary_expr>(split.remaining[1]);
  REQUIRE(membership);
  CHECK(membership->op == ast::binary_op::in);
  CHECK(is<ast::function_call>(membership->left));
}

TEST("select query rendering") {
  auto schema = make_schema();
  CHECK_EQUAL(make_select_query("db.tbl", nullptr, None{}, {}, None{}),
              std::string{"SELECT * FROM db.tbl"});
  CHECK_EQUAL(make_select_query("db.tbl", nullptr, None{}, {}, 42),
              std::string{"SELECT * FROM db.tbl LIMIT 42"});
  auto projection = ir::OptimizeProjection{};
  projection.push_back(*ast::field_path::try_from(parse("y")));
  projection.push_back(*ast::field_path::try_from(parse("meta.level")));
  projection.push_back(*ast::field_path::try_from(parse("x")));
  projection.push_back(*ast::field_path::try_from(parse("nope")));
  auto where = std::vector<std::string>{"`x` > 0", "`flag`"};
  // Columns come out in table order; a nested path narrows its top-level
  // column to the requested tuple elements; unknown fields are skipped.
  CHECK_EQUAL(make_select_query("tbl", &schema, projection, where, 5),
              std::string{"SELECT `x`, `y`, CAST(tuple(`meta`.`level`), "
                          "'Tuple(`level` Nullable(Int64))') AS `meta` FROM "
                          "tbl WHERE `x` > 0 AND `flag` LIMIT 5"});
  // A projection without a schema or without surviving columns selects all.
  CHECK_EQUAL(make_select_query("tbl", nullptr, projection, {}, None{}),
              std::string{"SELECT * FROM tbl"});
  auto unknown = ir::OptimizeProjection{};
  unknown.push_back(*ast::field_path::try_from(parse("nope")));
  CHECK_EQUAL(make_select_query("tbl", &schema, unknown, {}, None{}),
              std::string{"SELECT * FROM tbl"});
  // A projection that names a generated column keeps `*`, since whether
  // `SELECT *` includes it depends on session settings.
  auto generated = ir::OptimizeProjection{};
  generated.push_back(*ast::field_path::try_from(parse("id")));
  generated.push_back(*ast::field_path::try_from(parse("gen")));
  CHECK_EQUAL(make_select_query("tbl", &schema, generated, {}, 3),
              std::string{"SELECT * FROM tbl LIMIT 3"});
}

TEST("nested projections") {
  auto schema = SqlSchema{};
  schema.add_column("id", "UInt64");
  schema.add_column("m",
                    "Tuple(a String, b Nullable(Int64), c Tuple(d IPv4, "
                    "e LowCardinality(String)), `f g` Bool, h Tuple(Int64))");
  auto select = [&](std::initializer_list<std::string_view> fields) {
    auto projection = ir::OptimizeProjection{};
    for (auto field : fields) {
      projection.push_back(*ast::field_path::try_from(parse(field)));
    }
    return make_select_query("t", &schema, projection, {}, None{});
  };
  // Elements come out in declaration order, nested tuples are rebuilt
  // recursively, and names are quoted.
  CHECK_EQUAL(select({"m.c.e", "m.b", "id"}),
              std::string{"SELECT `id`, CAST(tuple(`m`.`b`, "
                          "CAST(tuple(`m`.`c`.`e`), 'Tuple(`e` "
                          "LowCardinality(String))')), 'Tuple(`b` "
                          "Nullable(Int64), `c` Tuple(`e` "
                          "LowCardinality(String)))') AS `m` FROM t"});
  CHECK_EQUAL(select({"m.c", "m.a"}),
              std::string{"SELECT CAST(tuple(`m`.`a`, `m`.`c`), 'Tuple(`a` "
                          "String, `c` Tuple(d IPv4,e "
                          "LowCardinality(String)))') AS `m` FROM t"});
  CHECK_EQUAL(select({"m[\"f g\"]"}),
              std::string{"SELECT CAST(tuple(`m`.`f g`), 'Tuple(`f g` "
                          "Bool)') AS `m` FROM t"});
  // The whole column, a path below it, or a path outside the schema select
  // the column as is.
  CHECK_EQUAL(select({"m", "m.a"}), std::string{"SELECT `m` FROM t"});
  CHECK_EQUAL(select({"m.a", "m.nope"}), std::string{"SELECT `m` FROM t"});
  CHECK_EQUAL(select({"m.h"}),
              std::string{"SELECT CAST(tuple(`m`.`h`), 'Tuple(`h` "
                          "Tuple(Int64))') AS `m` FROM t"});
  CHECK_EQUAL(select({"id.nope"}), std::string{"SELECT `id` FROM t"});
  // Narrowing a tuple that the operator cannot decode as a whole would make
  // the requested element decodable and change the result, so it stays whole.
  schema.add_column("half", "Tuple(ok String, bad Map(String, Int64))");
  CHECK_EQUAL(select({"half.ok"}), std::string{"SELECT `half` FROM t"});
  CHECK_EQUAL(select({"m.a", "half.ok"}),
              std::string{"SELECT CAST(tuple(`m`.`a`), 'Tuple(`a` String)') AS "
                          "`m`, `half` FROM t"});
}

TEST("decodable types") {
  CHECK(is_decodable_type("Int64"));
  CHECK(is_decodable_type("LowCardinality(Nullable(String))"));
  CHECK(is_decodable_type("Tuple(a String,b Tuple(c IPv4))"));
  CHECK(is_decodable_type("Array(UInt8)"));
  CHECK(is_decodable_type("Decimal(10, 2)"));
  CHECK(is_decodable_type("Enum8('a' = 1)"));
  CHECK(is_decodable_type("DateTime64(3, 'UTC')"));
  CHECK(not is_decodable_type("Map(String, Int64)"));
  CHECK(not is_decodable_type("Tuple(a String, b Map(String, Int64))"));
  CHECK(not is_decodable_type("Array(Map(String, Int64))"));
  CHECK(not is_decodable_type("Nonsense"));
  CHECK(not is_decodable_type(""));
}

TEST("quoting") {
  CHECK_EQUAL(quote_sql_identifier("plain"), std::string{"`plain`"});
  CHECK_EQUAL(quote_sql_identifier("with`tick"), std::string{"`with\\`tick`"});
  CHECK_EQUAL(quote_sql_identifier("back\\slash"),
              std::string{"`back\\\\slash`"});
  CHECK_EQUAL(quote_sql_string("it's"), std::string{"'it\\'s'"});
  CHECK_EQUAL(quote_sql_string("a\\b"), std::string{"'a\\\\b'"});
  CHECK_EQUAL(quote_sql_string("tab\there"), std::string{"'tab\\x09here'"});
  CHECK_EQUAL(quote_sql_string("esc\x1b\x7f"), std::string{"'esc\\x1B\\x7F'"});
  CHECK_EQUAL(quote_sql_string(std::string_view{"nul\0byte", 8}),
              std::string{"'nul\\x00byte'"});
  CHECK_EQUAL(quote_sql_string("ünïcödé"), std::string{"'ünïcödé'"});
}

} // namespace tenzir::plugins::clickhouse
