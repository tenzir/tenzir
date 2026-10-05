//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// Exact translations for predicates on the edges where TQL and ClickHouse
// could disagree: NaN and infinities, integers at the 2^53 boundary, a
// `Float32` value that `double` widens inexactly, out-of-range set elements,
// nulls, quotes, dates and timestamps in every ClickHouse precision, IPv4 and
// IPv6 addresses, enums, UUIDs, fixed strings, small-integer arithmetic, and
// IP addresses stored as strings.

#include "clickhouse/sql_pushdown.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/pushdown/translate.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <array>
#include <span>
#include <string_view>

namespace tenzir::plugins::clickhouse {

namespace {

/// A predicate and exactly what of it pushes, which is nothing if `pushed` is
/// empty.
struct Case {
  std::string_view predicate;
  std::string_view pushed;
};

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

auto numbers_schema() -> SqlSchema {
  auto schema = SqlSchema{};
  schema.add_column("id", "UInt64");
  schema.add_column("i", "Int64");
  schema.add_column("u", "UInt8");
  schema.add_column("f32", "Float32");
  schema.add_column("f64", "Float64");
  schema.add_column("n", "Nullable(Float64)");
  schema.add_column("s", "String");
  return schema;
}

constexpr auto numbers_cases = std::to_array<Case>({
  // Integer column vs literals around 2^53.
  Case{"i > 0", "`i` > 0"},
  Case{"i == 4503599627370496", "`i` = 4503599627370496"},
  Case{"i == 4503599627370496.0", "`i` = 4503599627370496."},
  Case{"i == 9007199254740992.0", ""},
  Case{"i > 9007199254740992.0", ""},
  Case{"i in [9007199254740993, 0]", "`i` IN (9007199254740993, 0)"},
  // A list with mixed literal types stays local: TQL nulls the clashing
  // elements, ClickHouse would match them.
  Case{"i in [1, 0.0]", ""},
  Case{"i in [0.0, 1]", ""},
  Case{"i in [0, 18446744073709551615]", ""},
  // UInt8 column vs out-of-range and negative literals.
  Case{"u in [44, 300]", "`u` IN (44, 300)"},
  Case{"u == 300", "`u` = 300"},
  Case{"u != 255", "`u` != 255"},
  Case{"u >= -1", "`u` >= -1"},
  // Float32 column: 0.1 widens inexactly, 16777217 is not representable.
  Case{"f32 == 0.1", "`f32` = 0.1"},
  Case{"f32 in [0.1, 0.5]", "`f32` IN (0.1, 0.5)"},
  Case{"f32 in [0.5, 16777216]", ""},
  // Integer literals compare with floating-point columns as doubles.
  Case{"f32 in [16777217]", "`f32` IN (16777217.)"},
  Case{"f32 == 16777217", "`f32` = 16777217."},
  Case{"f32 >= 16777216", "`f32` >= 16777216."},
  // Float64 column with nan and infinities.
  Case{"f64 > 0", "`f64` > 0."},
  Case{"f64 < 0", "`f64` < 0."},
  Case{"f64 != 1", "`f64` != 1."},
  Case{"f64 == 9007199254740992", "`f64` = 9007199254740992."},
  Case{"f64 == 9007199254740993", ""},
  Case{"f64 in [9007199254740992]", "`f64` IN (9007199254740992.)"},
  Case{"not (f64 > 0)", "NOT `f64` > 0."},
  // Nullable column: null-total equality, nan, ordering.
  Case{"n == null", "`n` IS NULL"},
  Case{"n != null", "`n` IS NOT NULL"},
  Case{"not (n == 1.5)", "NOT (`n` IS NOT NULL AND `n` = 1.5)"},
  Case{"n != 1.5", "(`n` IS NULL OR `n` != 1.5)"},
  Case{"n > 0", "`n` > 0."},
  Case{"n in [1.5, -1e300]", "(`n` IS NOT NULL AND `n` IN (1.5, -1e+300))"},
  // Strings with quotes and backslashes.
  Case{R"sql(s == "it's")sql", R"sql(`s` = 'it\'s')sql"},
  Case{R"sql(s in ["b\\c", ""])sql", R"sql(`s` IN ('b\\c', ''))sql"},
  Case{R"sql(s != "a")sql", "`s` != 'a'"},
  // Boolean combinators and a partially translatable conjunction.
  Case{"i > 0 or f64 < 0", "(`i` > 0 OR `f64` < 0.)"},
  Case{R"sql(i > 0 and s.starts_with("a"))sql", "`i` > 0 AND startsWith(`s`, "
                                                "'a')"},
  Case{R"sql(not (i > 0 and s == "a"))sql", "NOT (`i` > 0 AND `s` = 'a')"},
});

auto types_schema() -> SqlSchema {
  auto schema = SqlSchema{};
  schema.add_column("id", "UInt64");
  schema.add_column("d", "Date");
  schema.add_column("d2", "Date");
  schema.add_column("d32", "Date32");
  schema.add_column("dt", "DateTime('Asia/Tokyo')");
  schema.add_column("dt2", "Nullable(DateTime64(2))");
  schema.add_column("dt3", "DateTime64(3)");
  schema.add_column("dt3b", "DateTime64(3, 'Asia/Tokyo')");
  schema.add_column("dt9", "DateTime64(9)");
  schema.add_column("ip4", "IPv4");
  schema.add_column("ip6", "Nullable(IPv6)");
  schema.add_column(
    "e", R"sql(Enum8('low' = 1, 'high' = 2, 'a\\b' = 3, 'tab\there' = 4))sql");
  schema.add_column("half", "Tuple(ok String, bad Map(String, Int64))");
  schema.add_column("uid", "UUID");
  schema.add_column("uid2", "UUID");
  schema.add_column("fs", "FixedString(4)");
  schema.add_column("s", "String");
  schema.add_column("t", "LowCardinality(String)");
  schema.add_column("i32", "Int32");
  schema.add_column("u32", "Nullable(UInt32)");
  schema.add_column("n32", "Nullable(Int32)");
  schema.add_column("u8", "UInt8");
  schema.add_column("i64", "Int64");
  schema.add_column("f", "Float32");
  return schema;
}

constexpr auto types_cases = std::to_array<Case>({
  // `Date` against instants on a day, between days, and outside the range.
  Case{"d == 2024-01-01", "`d` = toDate('2024-01-01')"},
  Case{"d == 2024-01-01T12:00:00", "false"},
  Case{"d != 2024-01-01T12:00:00", "true"},
  Case{"d < 2024-01-01T12:00:00", "`d` <= toDate('2024-01-01')"},
  Case{"d >= 2024-01-01T12:00:00", "`d` > toDate('2024-01-01')"},
  Case{"d > 2150-01-01", "`d` > toDate('2149-06-06')"},
  Case{"d <= 2150-01-01", "`d` <= toDate('2149-06-06')"},
  Case{"d == 2150-01-01", "false"},
  Case{"d < 2024-01-01 - 100y", "`d` < toDate('1970-01-01')"},
  Case{"d in [2024-01-01, 2024-01-01T00:00:01, 2149-06-06]",
       "`d` IN (toDate('2024-01-01'), toDate('2149-06-06'))"},
  Case{"d == d2", "`d` = `d2`"},
  Case{"d < d2", "`d` < `d2`"},
  // `Date32` holds days past 2262 that TQL decodes to `null`.
  Case{"d32 > 2000-01-01", "(`d32` <= toDate32('2262-04-11') AND `d32` > "
                           "toDate32('2000-01-01'))"},
  Case{"d32 <= 2262-04-11", "(`d32` <= toDate32('2262-04-11') AND `d32` <= "
                            "toDate32('2262-04-11'))"},
  Case{"d32 != 2000-01-01", "`d32` != toDate32('2000-01-01')"},
  Case{"d32 == 1900-01-01", "`d32` = toDate32('1900-01-01')"},
  Case{"d32 == null", "if(`d32` <= toDate32('2262-04-11'), `d32`, NULL) IS "
                      "NULL"},
  Case{"d32 != null", "if(`d32` <= toDate32('2262-04-11'), `d32`, NULL) IS NOT "
                      "NULL"},
  Case{"not (d32 > 2000-01-01)", "NOT if(`d32` <= toDate32('2262-04-11'), "
                                 "`d32` > toDate32('2000-01-01'), NULL)"},
  Case{"not (d32 < 2000-01-01) or d32 == null",
       "(NOT if(`d32` <= toDate32('2262-04-11'), `d32` < "
       "toDate32('2000-01-01'), NULL) OR if(`d32` <= toDate32('2262-04-11'), "
       "`d32`, NULL) IS NULL)"},
  Case{"d32 > 2000-01-01 or d == 1970-01-01",
       "((`d32` <= toDate32('2262-04-11') AND `d32` > toDate32('2000-01-01')) "
       "OR `d` = toDate('1970-01-01'))"},
  Case{"not (dt3 < 2024-01-01)",
       "NOT if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3` < "
       "fromUnixTimestamp64Milli(1704067200000), NULL)"},
  Case{"not (dt3 > 2024-01-01)",
       "NOT if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3` > "
       "fromUnixTimestamp64Milli(1704067200000), NULL)"},
  // `DateTime` in a non-UTC zone compares by instant.
  Case{"dt == 2024-01-01T12:34:56", "`dt` = toDateTime('2024-01-01 12:34:56', "
                                    "'UTC')"},
  Case{"dt < 2024-01-01T12:34:56.5", "`dt` <= toDateTime('2024-01-01 "
                                     "12:34:56', 'UTC')"},
  Case{"dt >= 2107-01-01", "`dt` > toDateTime('2106-02-07 06:28:15', 'UTC')"},
  Case{"dt == 1970-01-01", "`dt` = toDateTime('1970-01-01 00:00:00', 'UTC')"},
  // `DateTime64(3)` holds instants past 2262 that TQL decodes to `null`.
  Case{"dt3 > 2024-01-01", "(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) "
                           "AND `dt3` <= "
                           "fromUnixTimestamp64Milli(9223372036854) AND `dt3` "
                           "> fromUnixTimestamp64Milli(1704067200000))"},
  Case{"dt3 < 2024-01-01", "(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) "
                           "AND `dt3` <= "
                           "fromUnixTimestamp64Milli(9223372036854) AND `dt3` "
                           "< fromUnixTimestamp64Milli(1704067200000))"},
  Case{"dt3 == 2024-01-01T00:00:00.123",
       "`dt3` = fromUnixTimestamp64Milli(1704067200123)"},
  Case{"dt3 != 2024-01-01T00:00:00.123",
       "`dt3` != fromUnixTimestamp64Milli(1704067200123)"},
  Case{"dt3 == 2024-01-01T00:00:00.1234", "false"},
  Case{"dt3 >= 2024-01-01T00:00:00.1234",
       "(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854) AND `dt3` > "
       "fromUnixTimestamp64Milli(1704067200123))"},
  Case{"dt3 == 1900-01-01", "`dt3` = fromUnixTimestamp64Milli(-2208988800000)"},
  Case{"dt3 == null", "if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) "
                      "AND `dt3` <= fromUnixTimestamp64Milli(9223372036854), "
                      "`dt3`, NULL) IS NULL"},
  Case{"dt3 != null", "if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) "
                      "AND `dt3` <= fromUnixTimestamp64Milli(9223372036854), "
                      "`dt3`, NULL) IS NOT NULL"},
  Case{"dt3 in [2024-01-01T00:00:00.123, 2024-01-01T00:00:00.1234]",
       "`dt3` IN (fromUnixTimestamp64Milli(1704067200123))"},
  Case{"dt3 == dt3b",
       "((if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL) IS NULL AND "
       "if(`dt3b` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3b` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3b`, NULL) IS NULL) OR "
       "(if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL) IS NOT NULL AND "
       "if(`dt3b` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3b` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3b`, NULL) IS NOT NULL AND "
       "if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL) = if(`dt3b` >= "
       "fromUnixTimestamp64Milli(-9223372036854) AND `dt3b` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3b`, NULL)))"},
  Case{"dt3 != dt3b",
       "NOT ((if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` "
       "<= fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL) IS NULL AND "
       "if(`dt3b` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3b` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3b`, NULL) IS NULL) OR "
       "(if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL) IS NOT NULL AND "
       "if(`dt3b` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3b` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3b`, NULL) IS NOT NULL AND "
       "if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL) = if(`dt3b` >= "
       "fromUnixTimestamp64Milli(-9223372036854) AND `dt3b` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3b`, NULL)))"},
  Case{"dt3 < dt3b", "if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND "
                     "`dt3` <= fromUnixTimestamp64Milli(9223372036854), `dt3`, "
                     "NULL) < if(`dt3b` >= "
                     "fromUnixTimestamp64Milli(-9223372036854) AND `dt3b` <= "
                     "fromUnixTimestamp64Milli(9223372036854), `dt3b`, NULL)"},
  Case{"dt3 <= dt3b",
       "((if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL) IS NULL AND "
       "if(`dt3b` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3b` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3b`, NULL) IS NULL) OR "
       "if(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3`, NULL) <= if(`dt3b` >= "
       "fromUnixTimestamp64Milli(-9223372036854) AND `dt3b` <= "
       "fromUnixTimestamp64Milli(9223372036854), `dt3b`, NULL))"},
  // `Nullable(DateTime64(2))` needs the generic literal form.
  Case{"dt2 == 2024-01-01T00:00:00.12",
       "(`dt2` IS NOT NULL AND `dt2` = "
       "CAST(fromUnixTimestamp64Nano(1704067200120000000), 'DateTime64(2)'))"},
  Case{"dt2 != 2024-01-01T00:00:00.12",
       "(`dt2` IS NULL OR `dt2` != "
       "CAST(fromUnixTimestamp64Nano(1704067200120000000), 'DateTime64(2)'))"},
  Case{"dt2 < 2024-01-01",
       "(`dt2` >= CAST(fromUnixTimestamp64Nano(-9223372036850000000), "
       "'DateTime64(2)') AND `dt2` <= "
       "CAST(fromUnixTimestamp64Nano(9223372036850000000), 'DateTime64(2)') "
       "AND `dt2` < CAST(fromUnixTimestamp64Nano(1704067200000000000), "
       "'DateTime64(2)'))"},
  Case{"dt2 == null", "if(`dt2` >= "
                      "CAST(fromUnixTimestamp64Nano(-9223372036850000000), "
                      "'DateTime64(2)') AND `dt2` <= "
                      "CAST(fromUnixTimestamp64Nano(9223372036850000000), "
                      "'DateTime64(2)'), `dt2`, NULL) IS NULL"},
  // `DateTime64(9)` decodes every tick.
  Case{"dt9 > 2024-01-01T00:00:00.000000001",
       "`dt9` > fromUnixTimestamp64Nano(1704067200000000001)"},
  Case{"dt9 == 1900-01-01", "`dt9` = "
                            "fromUnixTimestamp64Nano(-2208988800000000000)"},
  Case{"dt9 <= 2262-04-11T23:47:16.854775807",
       "`dt9` <= fromUnixTimestamp64Nano(9223372036854775807)"},
  // Different temporal types stay local.
  Case{"d == d32", ""},
  Case{"dt < dt3", ""},
  Case{"dt3 == dt9", ""},
  Case{R"sql(d == "2024-01-01")sql", ""},
  // `IPv4` against addresses of either family, and subnets.
  Case{"ip4 == 10.0.0.1", "`ip4` = toIPv4('10.0.0.1')"},
  Case{"ip4 == ::ffff:10.0.0.1", "`ip4` = toIPv4('10.0.0.1')"},
  Case{"ip4 == ::1", "false"},
  Case{"ip4 != ::1", "true"},
  Case{"ip4 < ::1", "`ip4` < toIPv4('0.0.0.0')"},
  Case{"ip4 >= ::1", "`ip4` >= toIPv4('0.0.0.0')"},
  Case{"ip4 <= 2001:db8::", "`ip4` <= toIPv4('255.255.255.255')"},
  Case{"ip4 > 2001:db8::", "`ip4` > toIPv4('255.255.255.255')"},
  Case{"ip4 > 192.168.0.0", "`ip4` > toIPv4('192.168.0.0')"},
  Case{"ip4 in 10.0.0.0/8", "`ip4` BETWEEN toIPv4('10.0.0.0') AND "
                            "toIPv4('10.255.255.255')"},
  Case{"ip4 in 192.168.1.4/30", "`ip4` BETWEEN toIPv4('192.168.1.4') AND "
                                "toIPv4('192.168.1.7')"},
  Case{"ip4 in ::/0", "`ip4` BETWEEN toIPv4('0.0.0.0') AND "
                      "toIPv4('255.255.255.255')"},
  Case{"ip4 in ::ffff:0:0/96", "`ip4` BETWEEN toIPv4('0.0.0.0') AND "
                               "toIPv4('255.255.255.255')"},
  Case{"ip4 in 2001:db8::/32", "`ip4` < toIPv4('0.0.0.0')"},
  Case{"ip4 in [10.0.0.1, ::1, 192.168.1.5]", "`ip4` IN (toIPv4('10.0.0.1'), "
                                              "toIPv4('192.168.1.5'))"},
  Case{"ip4 in [::1]", "false"},
  // `Nullable(IPv6)` against addresses of either family, and subnets.
  Case{"ip6 == 10.0.0.1", "(`ip6` IS NOT NULL AND `ip6` = toIPv6('10.0.0.1'))"},
  Case{"ip6 == ::ffff:10.0.0.1", "(`ip6` IS NOT NULL AND `ip6` = "
                                 "toIPv6('10.0.0.1'))"},
  Case{"ip6 != ::1", "(`ip6` IS NULL OR `ip6` != toIPv6('::1'))"},
  Case{"ip6 < 10.0.0.1", "`ip6` < toIPv6('10.0.0.1')"},
  Case{"ip6 >= 2001:db8::", "`ip6` >= toIPv6('2001:db8::')"},
  Case{"ip6 in 10.0.0.0/8", "`ip6` BETWEEN toIPv6('10.0.0.0') AND "
                            "toIPv6('10.255.255.255')"},
  Case{"ip6 in ::ffff:0:0/96", "`ip6` BETWEEN toIPv6('0.0.0.0') AND "
                               "toIPv6('255.255.255.255')"},
  Case{"ip6 in 2001:db8::/32", "`ip6` BETWEEN toIPv6('2001:db8::') AND "
                               "toIPv6('2001:db8:ffff:ffff:ffff:ffff:ffff:ffff'"
                               ")"},
  Case{"ip6 in ::/0", "`ip6` BETWEEN toIPv6('::') AND "
                      "toIPv6('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff')"},
  Case{"not (ip6 in 10.0.0.0/8)", "NOT `ip6` BETWEEN toIPv6('10.0.0.0') AND "
                                  "toIPv6('10.255.255.255')"},
  Case{"ip6 in [10.0.0.1, ::1]", "(`ip6` IS NOT NULL AND `ip6` IN "
                                 "(toIPv6('10.0.0.1'), toIPv6('::1')))"},
  Case{"ip4 == ip6", "(`ip6` IS NOT NULL AND `ip4` = `ip6`)"},
  Case{"ip4 != ip6", "NOT (`ip6` IS NOT NULL AND `ip4` = `ip6`)"},
  Case{"ip4 < ip6", "`ip4` < `ip6`"},
  Case{"ip4 <= ip6", "`ip4` <= `ip6`"},
  Case{R"sql(ip4 == "10.0.0.1")sql", ""},
  // A `String` column parses locally behind a prefilter; none of its values
  // here is an address.
  Case{"s in 10.0.0.0/8", "(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) "
                          "BETWEEN toIPv6('10.0.0.0') AND "
                          "toIPv6('10.255.255.255'))"},
  // Enums compare by name; an unknown name never matches.
  Case{R"sql(e == "high")sql", "`e` = 'high'"},
  Case{R"sql(e != "high")sql", "`e` != 'high'"},
  Case{R"sql(e == "nope")sql", "false"},
  Case{R"sql(e != "nope")sql", "true"},
  Case{R"sql(e in ["low", "nope"])sql", "`e` IN ('low')"},
  Case{R"sql(e in ["nope"])sql", "false"},
  Case{R"sql(e < "low")sql", ""},
  Case{"e == e", ""},
  // The decoder takes a label's rendering verbatim, backslashes included, so
  // TQL sees `a\\b` for the label `a\b`, and the literal is that rendering.
  Case{R"sql(e == "a\\\\b")sql", R"sql(`e` = 'a\\b')sql"},
  Case{R"sql(e == "a\\b")sql", "false"},
  Case{R"sql(e == "tab\\there")sql", R"sql(`e` = 'tab\there')sql"},
  Case{R"sql(e == "tab\there")sql", "false"},
  Case{R"sql(e in ["low", "a\\\\b"])sql", R"sql(`e` IN ('low', 'a\\b'))sql"},
  // A tuple with an element that does not decode is absent locally, so its
  // elements are neither compared in SQL nor narrowed by a projection.
  Case{R"sql(half.ok == "x")sql", ""},
  Case{R"sql(half.ok.starts_with("x"))sql", ""},
  // UUIDs match in canonical form only.
  Case{R"sql(uid == "61f0c404-5cb3-11e7-907b-a6006ad3dba0")sql",
       "`uid` = '61f0c404-5cb3-11e7-907b-a6006ad3dba0'"},
  Case{R"sql(uid != "61f0c404-5cb3-11e7-907b-a6006ad3dba0")sql",
       "`uid` != '61f0c404-5cb3-11e7-907b-a6006ad3dba0'"},
  Case{R"sql(uid == "61F0C404-5CB3-11E7-907B-A6006AD3DBA0")sql", "false"},
  Case{R"sql(uid == "not a uuid")sql", "false"},
  Case{
    R"sql(uid in ["61f0c404-5cb3-11e7-907b-a6006ad3dba0", "61F0C404-5CB3-11E7-907B-A6006AD3DBA0"])sql",
    "`uid` IN ('61f0c404-5cb3-11e7-907b-a6006ad3dba0')"},
  Case{R"sql(uid < "61f0c404-5cb3-11e7-907b-a6006ad3dba0")sql", ""},
  Case{"uid == uid2", "`uid` = `uid2`"},
  Case{"uid != uid2", "`uid` != `uid2`"},
  Case{"uid < uid2", ""},
  // Fixed strings keep their zero padding in TQL, so only a literal of the
  // full length can match.
  Case{R"sql(fs == "abcd")sql", "`fs` = 'abcd'"},
  Case{R"sql(fs != "abcd")sql", "`fs` != 'abcd'"},
  Case{R"sql(fs == "ab\u0000\u0000")sql", R"sql(`fs` = 'ab\x00\x00')sql"},
  Case{R"sql(fs == "ab")sql", "false"},
  Case{R"sql(fs != "ab")sql", "true"},
  Case{R"sql(fs == "abcde")sql", "false"},
  Case{R"sql(fs in ["ab", "abcd", "abcde"])sql", "`fs` IN ('abcd')"},
  Case{"fs.length_bytes() == 4", "length(`fs`) = 4"},
  Case{R"sql(fs < "b")sql", ""},
  Case{"fs == s", ""},
  // Strings order by bytes; functions with a direct counterpart translate.
  Case{R"sql(s < "b")sql", "`s` < 'b'"},
  Case{R"sql(s >= "b")sql", "`s` >= 'b'"},
  Case{R"sql(s > "z")sql", "`s` > 'z'"},
  Case{R"sql(s <= "")sql", "`s` <= ''"},
  Case{R"sql("b" in s)sql", "position(`s`, 'b') > 0"},
  Case{R"sql("" in s)sql", "position(`s`, '') > 0"},
  Case{R"sql(s.starts_with("a"))sql", "startsWith(`s`, 'a')"},
  Case{R"sql(s.ends_with("c"))sql", "endsWith(`s`, 'c')"},
  Case{R"sql(starts_with(s, ""))sql", "startsWith(`s`, '')"},
  Case{"s.length_bytes() > 2", "length(`s`) > 2"},
  Case{"s.length_bytes() in [0, 2]", "length(`s`) IN (0, 2)"},
  Case{"s == t", "`s` = `t`"},
  Case{"s != t", "`s` != `t`"},
  Case{"s < t", "`s` < `t`"},
  Case{"s <= t", "`s` <= `t`"},
  Case{R"sql(s.starts_with("A", ignore_case=true))sql", "startsWith(lowerUTF8(`"
                                                        "s`), lowerUTF8('A'))"},
  Case{R"sql(t.ends_with("É", ignore_case=true))sql", "endsWith(lowerUTF8(`t`),"
                                                      " lowerUTF8('É'))"},
  Case{"s.length_chars() > 2", ""},
  // Regular expressions search with RE2 on both sides. ClickHouse lets `.`
  // match a newline unless the pattern starts with `(?-s)`.
  Case{R"sql(s.match_regex("^a.c"))sql", "match(`s`, '(?-s)^a.c')"},
  Case{R"sql(s.match_regex("(?i)B"))sql", "match(`s`, '(?-s)(?i)B')"},
  Case{R"sql(s.match_regex("^.$"))sql", "match(`s`, '(?-s)^.$')"},
  Case{R"sql(s.match_regex(""))sql", "match(`s`, '(?-s)')"},
  Case{R"sql(not t.match_regex("c$"))sql", "NOT match(`t`, '(?-s)c$')"},
  // Arithmetic on small integers and floats; overflow-prone forms stay local.
  Case{"i32 + 1 > 0", "(`i32` + 1) > 0"},
  Case{"i32 - 1 < 0", "(`i32` - 1) < 0"},
  Case{"i32 * -2 < 0", "(`i32` * -2) < 0"},
  Case{"i32 * 2147483647 > 0", "(`i32` * 2147483647) > 0"},
  Case{"i32 / 2 == 1.5", "(`i32` / 2) = 1.5"},
  // A `double` literal keeps its fraction so that arithmetic stays in
  // `Float64`; the row with `i64 = 2^53 + 1` rounds in TQL and must in SQL.
  Case{"i64 + 0.0 == 9007199254740992", "(`i64` + 0.) = 9007199254740992."},
  Case{"i64 * 1.0 == 9223372036854775807", ""},
  Case{"u8 + 0.0 == 255", "(`u8` + 0.) = 255."},
  Case{"i32 * 2.0 > 3", "(`i32` * 2.) > 3."},
  Case{"i32 / 2 > 1", "(`i32` / 2) > 1."},
  Case{"1 + i32 == 4", "(1 + `i32`) = 4"},
  Case{"10 - i32 == 7", "(10 - `i32`) = 7"},
  Case{"u8 + 1 == 256", "(`u8` + 1) = 256"},
  Case{"u8 * 2 == 510", "(`u8` * 2) = 510"},
  Case{"u8 - -1 == 256", "(`u8` - -1) = 256"},
  Case{"u32 - -1 == 1", "((`u32` - -1) IS NOT NULL AND (`u32` - -1) = 1)"},
  Case{"5 - u32 < 0", "(5 - `u32`) < 0"},
  Case{"u32 / 2 > 1", "(`u32` / 2) > 1."},
  Case{"i32 + 0.5 > 3", "(`i32` + 0.5) > 3."},
  Case{"f + 1 > 1.1", "(`f` + 1) > 1.1"},
  Case{"f * 3 == 1.5", "(`f` * 3) = 1.5"},
  Case{"f / 0.5 >= 1", "(`f` / 0.5) >= 1."},
  Case{"2 - f > 1", "(2 - `f`) > 1."},
  Case{"i32 + 1 == 2147483648", "(`i32` + 1) = 2147483648"},
  Case{"i32 - 1 == -2147483649", "(`i32` - 1) = -2147483649"},
  Case{"u32 - 1 == 0", ""},
  Case{"u32 + -1 == 0", ""},
  Case{"u8 - 1 < 0", ""},
  Case{"i32 + 2147483648 > 0", ""},
  Case{"i32 + u8 > 0", ""},
  Case{"i64 + 1 > 0", ""},
  Case{"i32 / 0 > 1", ""},
  Case{"1 / i32 > 1", ""},
  // Literal expressions fold before translation.
  Case{"i32 > 1024 * 1024", "`i32` > 1048576"},
  Case{"i32 > -(1 + 2)", "`i32` > -3"},
  Case{"dt3 > 2024-01-01 - 1d",
       "(`dt3` >= fromUnixTimestamp64Milli(-9223372036854) AND `dt3` <= "
       "fromUnixTimestamp64Milli(9223372036854) AND `dt3` > "
       "fromUnixTimestamp64Milli(1703980800000))"},
  Case{"i32 > 9223372036854775807 + 1", ""},
  Case{"dt3 > now() - 10y", ""},
  // Comparisons between two integer columns, nullable or not.
  Case{"i32 < u32", "`i32` < `u32`"},
  Case{"i32 == u32", "(`u32` IS NOT NULL AND `i32` = `u32`)"},
  Case{"i32 != u32", "NOT (`u32` IS NOT NULL AND `i32` = `u32`)"},
  Case{"i32 <= u32", "`i32` <= `u32`"},
  Case{"u32 == n32", "((`u32` IS NULL AND `n32` IS NULL) OR (`u32` IS NOT NULL "
                     "AND `n32` IS NOT NULL AND `u32` = `n32`))"},
  Case{"u32 != n32", "NOT ((`u32` IS NULL AND `n32` IS NULL) OR (`u32` IS NOT "
                     "NULL AND `n32` IS NOT NULL AND `u32` = `n32`))"},
  Case{"u32 <= n32", "((`u32` IS NULL AND `n32` IS NULL) OR `u32` <= `n32`)"},
  Case{"u32 >= n32", "((`u32` IS NULL AND `n32` IS NULL) OR `u32` >= `n32`)"},
  Case{"u32 < n32", "`u32` < `n32`"},
  Case{"not (u32 <= n32)", "NOT ((`u32` IS NULL AND `n32` IS NULL) OR `u32` <= "
                           "`n32`)"},
  Case{"i32 + 1 < u32", "(`i32` + 1) < `u32`"},
  Case{"i64 < u32", "`i64` < `u32`"},
  Case{"i32 < f", ""},
});

auto strings_schema() -> SqlSchema {
  auto schema = SqlSchema{};
  schema.add_column("id", "UInt64");
  schema.add_column("s", "String");
  schema.add_column("ns", "Nullable(String)");
  schema.add_column("lc", "LowCardinality(String)");
  return schema;
}

constexpr auto strings_cases = std::to_array<Case>({
  // An `ip` literal against a `String` column compares against its text.
  Case{"s == 1.1.1.1", "`s` = '1.1.1.1'"},
  Case{"1.1.1.1 == s", "`s` = '1.1.1.1'"},
  Case{"s != 1.1.1.1", "`s` != '1.1.1.1'"},
  Case{"s == 2001:DB8::1", "`s` = '2001:db8::1'"},
  Case{"s == ::ffff:1.1.1.1", "`s` = '1.1.1.1'"},
  Case{"s in [1.1.1.1, ::1, 10.0.0.1]", "`s` IN ('1.1.1.1', '::1', "
                                        "'10.0.0.1')"},
  Case{"ns == 1.1.1.1", "(`ns` IS NOT NULL AND `ns` = '1.1.1.1')"},
  Case{"ns != 1.1.1.1", "(`ns` IS NULL OR `ns` != '1.1.1.1')"},
  Case{"lc == 10.0.0.1", "`lc` = '10.0.0.1'"},
  Case{"not (s == 1.1.1.1 or id > 20)", "NOT (`s` = '1.1.1.1' OR `id` > 20)"},
  // Ordering has no textual counterpart and keeps TQL's outcome: no rows.
  Case{"s < 1.1.1.1", ""},
  // Membership in a subnet parses the column, with a prefilter in SQL.
  Case{"s in 10.0.0.0/8", "(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) "
                          "BETWEEN toIPv6('10.0.0.0') AND "
                          "toIPv6('10.255.255.255'))"},
  Case{"s.ip() in 10.0.0.0/8", "(toIPv6OrNull(`s`) IS NULL OR "
                               "toIPv6OrNull(`s`) BETWEEN toIPv6('10.0.0.0') "
                               "AND toIPv6('10.255.255.255'))"},
  Case{"ns in 10.0.0.0/8", "(toIPv6OrNull(`ns`) IS NULL OR toIPv6OrNull(`ns`) "
                           "BETWEEN toIPv6('10.0.0.0') AND "
                           "toIPv6('10.255.255.255'))"},
  Case{"s in ::ffff:0:0/96", "(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) "
                             "BETWEEN toIPv6('0.0.0.0') AND "
                             "toIPv6('255.255.255.255'))"},
  Case{"s in 2001:db8::/32", "(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) "
                             "BETWEEN toIPv6('2001:db8::') AND "
                             "toIPv6('2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')"
                             ")"},
  Case{"s in ::/0", "(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) BETWEEN "
                    "toIPv6('::') AND "
                    "toIPv6('ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'))"},
  Case{"s in 1.1.1.1/32", "(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) "
                          "BETWEEN toIPv6('1.1.1.1') AND toIPv6('1.1.1.1'))"},
  // An explicit parse gets the same prefilter for equality and ordering.
  Case{"s.ip() == 1.1.1.1", "(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) = "
                            "toIPv6('1.1.1.1'))"},
  Case{"s.ip() == ::1", "(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) = "
                        "toIPv6('::1'))"},
  Case{"s.ip() in [1.1.1.1, ::1]", "(toIPv6OrNull(`s`) IS NULL OR "
                                   "toIPv6OrNull(`s`) IN (toIPv6('1.1.1.1'), "
                                   "toIPv6('::1')))"},
  Case{"s.ip() < 10.0.0.0", "(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) < "
                            "toIPv6('10.0.0.0'))"},
  Case{"s.ip() >= 2001:db8::", "(toIPv6OrNull(`s`) IS NULL OR "
                               "toIPv6OrNull(`s`) >= toIPv6('2001:db8::'))"},
  // `!=` and `not` keep rows TQL cannot parse, so they get no prefilter.
  Case{"s.ip() != 1.1.1.1", ""},
  Case{"not (s in 10.0.0.0/8)", ""},
  // Prefilters compose.
  Case{"s in 10.0.0.0/8 or s == 1.1.1.1",
       "((toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) BETWEEN "
       "toIPv6('10.0.0.0') AND toIPv6('10.255.255.255')) OR `s` = '1.1.1.1')"},
  Case{"(s in 10.0.0.0/8 and s.length_chars() > 8) or id == 1",
       "((toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) BETWEEN "
       "toIPv6('10.0.0.0') AND toIPv6('10.255.255.255')) OR `id` = 1)"},
  Case{"s in 10.0.0.0/8 and id < 100",
       "(toIPv6OrNull(`s`) IS NULL OR toIPv6OrNull(`s`) BETWEEN "
       "toIPv6('10.0.0.0') AND toIPv6('10.255.255.255')) AND `id` < 100"},
  Case{"s in 10.0.0.0/8 or s.length_chars() > 8", ""},
  // Without `(?-s)`, `.` would match the trailing newline of row 32, and
  // `$` matches only at the very end, as in RE2.
  Case{R"sql(lc.match_regex("^1.1.1.1."))sql", "match(`lc`, '(?-s)^1.1.1.1.')"},
  Case{R"sql(lc.match_regex("(?s)^1.1.1.1."))sql", "match(`lc`, "
                                                   "'(?-s)(?s)^1.1.1.1.')"},
  Case{R"sql(lc.match_regex("^1.1.1.1$"))sql", "match(`lc`, '(?-s)^1.1.1.1$')"},
  Case{R"sql(s.match_regex("^\\d+\\."))sql",
       R"sql(match(`s`, '(?-s)^\\d+\\.'))sql"},
  // A string literal is a plain string comparison, as before.
  Case{R"sql(s == "1.1.1.1")sql", "`s` = '1.1.1.1'"},
  Case{R"sql(s in ["1.1.1.1", "::1"])sql", "`s` IN ('1.1.1.1', '::1')"},
});

auto check_pushdown(SqlSchema const& schema, std::span<Case const> cases)
  -> void {
  for (auto const& x : cases) {
    auto filter = ir::OptimizeFilter{};
    filter.push_back(parse(x.predicate));
    auto split = pushdown::split_filter(std::move(filter), schema.model(),
                                        ClickHouseRenderer{});
    CHECK_EQUAL(fmt::format("{}: {}", x.predicate,
                            fmt::join(split.pushed, " AND ")),
                fmt::format("{}: {}", x.predicate, x.pushed));
  }
}

} // namespace

TEST("pushdown of numbers and plain strings") {
  check_pushdown(numbers_schema(), numbers_cases);
}

TEST("pushdown of temporal, address, enum, and other types") {
  check_pushdown(types_schema(), types_cases);
}

TEST("pushdown of ip literals against string columns") {
  check_pushdown(strings_schema(), strings_cases);
}

} // namespace tenzir::plugins::clickhouse
