//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/kql.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <fmt/format.h>

#include <limits>
#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::pushdown;

namespace {

auto parse(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  if (not expr) {
    FAIL("failed to parse: {}", source);
  }
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ctx));
  return std::move(*expr);
}

auto filters(std::vector<std::string_view> sources) -> ir::OptimizeFilter {
  auto result = ir::OptimizeFilter{};
  for (auto source : sources) {
    result.push_back(parse(source));
  }
  return result;
}

auto paths(std::vector<std::string_view> sources) -> ir::OptimizeProjection {
  auto result = ir::OptimizeProjection{};
  for (auto source : sources) {
    auto path = ast::field_path::try_from(parse(source));
    REQUIRE(path);
    result.push_back(std::move(*path));
  }
  return result;
}

/// The number of rows after which the service truncates a result.
constexpr auto cap = uint64_t{1000};

/// The appended text for a single predicate, or the empty string if nothing
/// was pushed.
auto where(std::string_view predicate) -> std::string {
  auto plan = plan_kql_query("T", filters({predicate}), None{}, None{}, cap,
                             test::builtin_registry());
  if (not plan.filtered) {
    return {};
  }
  constexpr auto prefix = std::string_view{"T\n| take 1000\n| where "};
  REQUIRE(plan.query.starts_with(prefix));
  return plan.query.substr(prefix.size());
}

constexpr auto x = std::string_view{"column_ifexists('x', dynamic(null))"};

auto string_guard(std::string_view condition) -> std::string {
  return fmt::format("(gettype({0}) !in (\"string\") or {1} or tostring({0}) "
                     "matches regex @'^[ \\t\\r\\n]*\"')",
                     x, condition);
}

auto number_guard(std::string_view condition) -> std::string {
  return fmt::format("(gettype({}) !in (\"int\", \"long\", \"real\", "
                     "\"double\") or {})",
                     x, condition);
}

} // namespace

TEST("kql renders string equality only where kql sees a string") {
  CHECK_EQUAL(where(R"(x == "a\"b\\c")"),
              string_guard(fmt::format(R"(tostring({}) == "a\"b\\c")", x)));
  CHECK_EQUAL(where(R"("Straße" == x)"),
              string_guard(fmt::format(R"(tostring({}) == "Straße")", x)));
  CHECK_EQUAL(where(R"(x == "")"),
              string_guard(fmt::format(R"(tostring({}) == "")", x)));
  // Control characters use KQL's escapes.
  CHECK_EQUAL(where("x == \"a\\nb\""),
              string_guard(fmt::format(R"(tostring({}) == "a\nb")", x)));
  CHECK_EQUAL(
    where("x == \"\\t\\r\\u{1}\\u{7f}\""),
    string_guard(fmt::format(R"(tostring({}) == "\t\r\u0001\u007f")", x)));
  // Unicode line separators are escaped, so that the literal stays on one
  // line and the planned query still lexes.
  CHECK_EQUAL(where("x == \"a\\u{2028}b\\u{2029}c\\u{85}d\""),
              string_guard(
                fmt::format(R"(tostring({}) == "a\u2028b\u2029c\u0085d")", x)));
  auto separated
    = plan_kql_query("T", filters({"x == \"a\\u{2028}b\\u{2029}c\\u{85}d\""}),
                     None{}, None{}, cap, test::builtin_registry());
  CHECK(separated.filtered);
  CHECK(kql_accepts_trailing_stages(separated.query));
  // Strings have no ordering operators in KQL.
  CHECK_EQUAL(where(R"(x < "b")"), "");
  // `!=` must also keep `null`, which a dynamic column cannot prove.
  CHECK_EQUAL(where(R"(x != "a")"), "");
}

TEST("kql renders number comparisons in real") {
  CHECK_EQUAL(where("x == 42"),
              number_guard(fmt::format("todouble({}) == 42.0", x)));
  CHECK_EQUAL(where("x > -1.5"),
              number_guard(fmt::format("todouble({}) > -1.5", x)));
  CHECK_EQUAL(where("3 <= x"),
              number_guard(fmt::format("todouble({}) >= 3.0", x)));
  // The boundary of exact doubles relaxes strict comparisons.
  CHECK_EQUAL(
    where("x > 9007199254740992"),
    number_guard(fmt::format("todouble({}) >= 9007199254740992.0", x)));
  CHECK_EQUAL(
    where("x < -9007199254740992"),
    number_guard(fmt::format("todouble({}) <= -9007199254740992.0", x)));
  CHECK_EQUAL(
    where("x == 9007199254740992"),
    number_guard(fmt::format("todouble({}) == 9007199254740992.0", x)));
  CHECK_EQUAL(
    where("x <= 9007199254740991"),
    number_guard(fmt::format("todouble({}) <= 9007199254740991.0", x)));
  // Literals beyond 2^53 do not convert to `real` exactly.
  CHECK_EQUAL(where("x > 9007199254740993"), "");
  CHECK_EQUAL(where("x < -9007199254740993"), "");
  CHECK_EQUAL(where("x == 9223372036854775807"), "");
  CHECK_EQUAL(where("x == 18446744073709551615"), "");
  CHECK_EQUAL(where("x > 9007199254740994.0"), "");
  CHECK_EQUAL(where("x in [1, 9007199254740993]"), "");
  // Exponents use the shortest spelling.
  CHECK_EQUAL(where("x > 0.0000001"),
              number_guard(fmt::format("todouble({}) > 1e-7", x)));
  CHECK_EQUAL(where("x < -0.00000015"),
              number_guard(fmt::format("todouble({}) < -1.5e-7", x)));
  CHECK_EQUAL(where("x > 1e300"), "");
}

TEST("kql renders booleans only where kql sees a boolean") {
  auto expected
    = fmt::format("(gettype({0}) !in (\"bool\") or tobool({0}) == true)", x);
  CHECK_EQUAL(where("x == true"), expected);
  CHECK_EQUAL(where("x"), expected);
  CHECK_EQUAL(where("x == false"),
              fmt::format("(gettype({0}) !in (\"bool\") or tobool({0}) == "
                          "false)",
                          x));
}

TEST("kql renders membership by kind") {
  CHECK_EQUAL(where(R"(x in ["a", "b"])"),
              string_guard(fmt::format(R"(tostring({}) in ("a", "b"))", x)));
  CHECK_EQUAL(where("x in [1, 2]"),
              number_guard(fmt::format("todouble({0}) == 1.0 or todouble({0}) "
                                       "== 2.0",
                                       x)));
  CHECK_EQUAL(where("x in [true]"),
              fmt::format("(gettype({0}) !in (\"bool\") or tobool({0}) == "
                          "true)",
                          x));
  // A dynamic string `443` decodes to the number 443, so no type guard of a
  // list that mixes kinds can restrict the rows of another kind: such lists
  // stay local, in every element order.
  CHECK_EQUAL(where(R"(x in ["a", 443])"), "");
  CHECK_EQUAL(where(R"(x in [443, "a"])"), "");
  CHECK_EQUAL(where(R"(x in ["a", true])"), "");
  CHECK_EQUAL(where(R"(x in [true, "a"])"), "");
  CHECK_EQUAL(where("x in [1, true]"), "");
  CHECK_EQUAL(where(R"(x in ["a", 1, true])"), "");
  // A list with `null` stays local.
  CHECK_EQUAL(where("x in [1, null]"), "");
}

TEST("kql renders case-sensitive string matching") {
  CHECK_EQUAL(
    where(R"(x.starts_with("ab"))"),
    string_guard(fmt::format(R"(tostring({}) startswith_cs "ab")", x)));
  CHECK_EQUAL(where(R"(x.ends_with("ab"))"),
              string_guard(fmt::format(R"(tostring({}) endswith_cs "ab")", x)));
  CHECK_EQUAL(where(R"("ab" in x)"),
              string_guard(fmt::format(R"(tostring({}) contains_cs "ab")", x)));
  // Matching is ordinal, so empty and non-ASCII needles are pushed, too.
  CHECK_EQUAL(where(R"(x.starts_with(""))"),
              string_guard(fmt::format(R"(tostring({}) startswith_cs "")", x)));
  CHECK_EQUAL(where(R"(x.ends_with("ße"))"),
              string_guard(fmt::format(R"(tostring({}) endswith_cs "ße")", x)));
  CHECK_EQUAL(
    where(R"("e\u{301}" in x)"),
    string_guard(fmt::format("tostring({}) contains_cs \"e\u0301\"", x)));
  // Non-ASCII needles remain local when case is ignored.
  CHECK_EQUAL(where(R"(x.starts_with("ä", ignore_case=true))"), "");
}

TEST("kql guards ASCII case-insensitive string prefilters") {
  auto guarded = [](std::string_view op, std::string_view needle,
                    bool recursive = false) {
    auto condition
      = fmt::format("tostring({0}) {1} \"{2}\" or tostring({0}) matches regex "
                    "@'[^\\x00-\\x7F]'",
                    x, op, needle);
    if (recursive) {
      condition += fmt::format(
        " or tostring({}) matches regex @'^[ \\t\\r\\n]*[\\[{{]'", x);
    }
    return string_guard(condition);
  };
  CHECK_EQUAL(where(R"(x.starts_with("ab", ignore_case=true))"),
              guarded("startswith", "ab"));
  CHECK_EQUAL(where(R"(x.ends_with("AB", ignore_case=true))"),
              guarded("endswith", "AB"));
  CHECK_EQUAL(where(R"(x.to_lower() == "alice")"), guarded("=~", "alice"));
  CHECK_EQUAL(where(R"("ALICE" == x.to_upper())"), guarded("=~", "ALICE"));
  CHECK_EQUAL(where(R"(x.to_lower() in ["alice", "bob"])"),
              fmt::format("({} or {})", guarded("=~", "alice"),
                          guarded("=~", "bob")));
  CHECK_EQUAL(where(R"(search(x, "powershell", ignore_case=true))"),
              guarded("contains", "powershell", true));
  CHECK_EQUAL(where(R"(x.contains("-enc", ignore_case=true))"),
              guarded("contains", "-enc", true));
  CHECK_EQUAL(where(R"(x.search("ALICE", exact=true, ignore_case=true))"),
              guarded("=~", "ALICE", true));
  CHECK_EQUAL(where(R"(x.search("", ignore_case=true))"),
              guarded("contains", "", true));
  CHECK_EQUAL(where(R"(x.search("ß", ignore_case=true))"), "");
  CHECK_EQUAL(where(R"(x.to_upper() in ["A", "Ä"])"), "");
  CHECK_EQUAL(where(R"(x.search("a", ignore_case=true) or x.match_regex("b"))"),
              "");
  CHECK_EQUAL(where(R"(search(this, "a", ignore_case=true))"), "");
}

TEST("kql compares parsed IPs and subnet bounds in the mapped address space") {
  auto guard = fmt::format("isempty(parse_ipv6(tostring({})))", x);
  auto eq = [&](std::string_view address) {
    return fmt::format("({} or ipv6_compare(tostring({}), \"{}\") == 0)", guard,
                       x, address);
  };
  CHECK_EQUAL(where("x.ip() == 10.0.0.1"), eq("10.0.0.1"));
  CHECK_EQUAL(where("x == 10.0.0.1"), eq("10.0.0.1"));
  CHECK_EQUAL(where("::ffff:10.0.0.1 == x.ip()"), eq("10.0.0.1"));
  CHECK_EQUAL(where("x.ip() == 2001:db8::1"), eq("2001:db8::1"));
  auto subnet = [&](std::string_view lo, std::string_view hi) {
    return fmt::format("({0} or (ipv6_compare(tostring({1}), \"{2}\") >= 0 "
                       "and ipv6_compare(tostring({1}), \"{3}\") <= 0))",
                       guard, x, lo, hi);
  };
  CHECK_EQUAL(where("x.ip() in 10.20.7.8/16"),
              subnet("10.20.0.0", "10.20.255.255"));
  CHECK_EQUAL(where("x in 0.0.0.0/0"), subnet("0.0.0.0", "255.255.255.255"));
  CHECK_EQUAL(where("x.ip() in 10.0.0.1/32"), subnet("10.0.0.1", "10.0.0.1"));
  // KQL aliases compatible and mapped IPv4 addresses. Moving a subnet bound
  // between those regions can exclude otherwise matching native addresses.
  for (auto network :
       {"::/0", "::/80", "::/96", "::fffe:0:0/95", "::a00:1/128"}) {
    CHECK_EQUAL(where(fmt::format("x.ip() in {}", network)), "");
    CHECK_EQUAL(where(fmt::format("x.ip() in {} or x == 1", network)), "");
  }
  CHECK_EQUAL(where("x.ip() in 2001:db8::1/128"),
              subnet("2001:db8::1", "2001:db8::1"));
  CHECK_EQUAL(where("x.ip() in 2001:db8::/32"),
              subnet("2001:db8::", "2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"));
  CHECK_EQUAL(where("x.ip() == 10.0.0.1 or x.match_regex(\"a\")"), "");
}

TEST("kql renders null checks for strictly decoded types only") {
  CHECK_EQUAL(where("x != null"),
              fmt::format("(isnotnull({0}) or gettype({0}) !in (\"int\", "
                          "\"long\", \"real\", \"bool\", \"datetime\", "
                          "\"timespan\", \"decimal\"))",
                          x));
  CHECK_EQUAL(where("x == null"), "");
}

TEST("kql vetoes predicates without a proven superset") {
  CHECK_EQUAL(where("not (x == 1)"), "");
  CHECK_EQUAL(where("x.y == 1"), "");
  CHECK_EQUAL(where(R"(x.match_regex("a"))"), "");
  CHECK_EQUAL(where("x.ip() != 1.2.3.4"), "");
  CHECK_EQUAL(where("x.ip() > 1.2.3.4"), "");
  CHECK_EQUAL(where("x > 2026-01-01"), "");
  CHECK_EQUAL(where("x + 1 > 2"), "");
  CHECK_EQUAL(where("x == y"), "");
  CHECK_EQUAL(where(R"(x.to_lower() == "ä")"), "");
  CHECK_EQUAL(where(R"(x.to_lower() != "a")"), "");
  CHECK_EQUAL(where("int(x, base=16) == 16"), "");
}

TEST("kql composes junctions") {
  CHECK_EQUAL(where("x == 1 or x == 2"),
              fmt::format("({} or {})",
                          number_guard(fmt::format("todouble({}) == 1.0", x)),
                          number_guard(fmt::format("todouble({}) == 2.0", x))));
  // A disjunction needs both sides.
  CHECK_EQUAL(where("x == 1 or x.match_regex(\"a\")"), "");
  // A conjunction inside a disjunction drops its local-only part.
  CHECK_EQUAL(where("(x == 1 and x.match_regex(\"a\")) or x == 2"),
              fmt::format("({} or {})",
                          number_guard(fmt::format("todouble({}) == 1.0", x)),
                          number_guard(fmt::format("todouble({}) == 2.0", x))));
}

TEST("kql plans append stages in order") {
  auto filter = filters({R"(x == 1 and y.match_regex("a"))", R"(z == "b")"});
  auto plan
    = plan_kql_query("T | take 5 // comment", filter, paths({"x", "w.v", "x"}),
                     uint64_t{3}, cap, test::builtin_registry());
  CHECK(plan.filtered);
  CHECK(plan.projected);
  // Every predicate stays local, so the limit is not pushed.
  CHECK(not plan.limited);
  auto z = std::string_view{"column_ifexists('z', dynamic(null))"};
  auto expected
    = fmt::format("T | take 5 // comment\n"
                  "| take 1000\n"
                  "| where {0} and (gettype({1}) !in (\"string\") or "
                  "tostring({1}) == \"b\" or "
                  "tostring({1}) matches regex @'^[ \\t\\r\\n]*\"')\n"
                  "| project ['x'] = column_ifexists('x', dynamic(null)), "
                  "['w'] = column_ifexists('w', dynamic(null)), "
                  "['y'] = column_ifexists('y', dynamic(null)), "
                  "['z'] = column_ifexists('z', dynamic(null))",
                  number_guard(fmt::format("todouble({}) == 1.0", x)), z);
  CHECK_EQUAL(plan.query, expected);
}

TEST("kql plans cap the query before filtering") {
  // Without the cap, the filter would reveal rows beyond the service's result
  // cap, which a sorted query would never return.
  auto filter = filters({"x == 1"});
  auto plan = plan_kql_query("T | sort by y", filter, None{}, uint64_t{5},
                             uint64_t{100'000}, test::builtin_registry());
  CHECK_EQUAL(plan.query,
              fmt::format("T | sort by y\n| take 100000\n| where {}",
                          number_guard(fmt::format("todouble({}) == 1.0", x))));
  CHECK(plan.filtered);
  CHECK(not plan.limited);
  // A service that fails a truncated result needs no cap: the uncapped filter
  // yields the complete filtered result or fails, too.
  plan = plan_kql_query("T | sort by y", filter, None{}, None{}, None{},
                        test::builtin_registry());
  CHECK_EQUAL(plan.query,
              fmt::format("T | sort by y\n| where {}",
                          number_guard(fmt::format("todouble({}) == 1.0", x))));
  CHECK(plan.filtered);
  // A projection or a limit keeps the rows of the query, or a prefix.
  plan = plan_kql_query("T | sort by y", {}, paths({"a"}), uint64_t{5},
                        uint64_t{100'000}, test::builtin_registry());
  CHECK_EQUAL(plan.query, "T | sort by y\n| project ['a'] = "
                          "column_ifexists('a', dynamic(null))\n| take 5");
  // A filter without prefilters needs no cap either.
  plan = plan_kql_query("T", filters({R"(x.match_regex("a"))"}), paths({"x"}),
                        None{}, uint64_t{100'000}, test::builtin_registry());
  CHECK_EQUAL(plan.query,
              "T\n| project ['x'] = column_ifexists('x', dynamic(null))");
  // A `set` statement may change how the service limits a result, so the
  // query stays unchanged.
  for (auto query : {"set notruncation;\nT", "set truncationmaxrecords=10; T",
                     "set query_take_max_records = 1000; T"}) {
    CHECK(not kql_accepts_trailing_stages(query));
    CHECK_EQUAL(plan_kql_query(query, filter, None{}, None{}, cap,
                               test::builtin_registry())
                  .query,
                query);
  }
}

TEST("kql plans push limits without filters") {
  auto plan = plan_kql_query("  T\n| project a  ", {}, None{}, uint64_t{7}, cap,
                             test::builtin_registry());
  CHECK_EQUAL(plan.query, "  T\n| project a  \n| take 7");
  CHECK(plan.limited);
  CHECK(not plan.filtered);
  CHECK(not plan.projected);
  // KQL spells counts as `long`, so only limits up to 2^63 - 1 are pushed.
  for (auto [limit, take] :
       {std::pair{uint64_t{2147483648}, "2147483648"},
        std::pair{uint64_t{9223372036854775807}, "9223372036854775807"}}) {
    plan
      = plan_kql_query("T", {}, None{}, limit, cap, test::builtin_registry());
    CHECK_EQUAL(plan.query, fmt::format("T\n| take {}", take));
    CHECK(plan.limited);
  }
  for (auto limit : {uint64_t{0}, uint64_t{9223372036854775808u},
                     std::numeric_limits<uint64_t>::max()}) {
    plan
      = plan_kql_query("T", {}, None{}, limit, cap, test::builtin_registry());
    CHECK_EQUAL(plan.query, "T");
    CHECK(not plan.limited);
  }
  // A filter that renders nothing still runs locally after the limit.
  plan = plan_kql_query("T", filters({R"(x.match_regex("a"))"}), None{},
                        uint64_t{7}, cap, test::builtin_registry());
  CHECK_EQUAL(plan.query, "T");
  CHECK(not plan.limited);
}

TEST("kql plans keep projection dependencies") {
  // A filter whose references cannot be enumerated disables projection.
  auto plan = plan_kql_query("T", filters({"this == {}"}), paths({"a"}), None{},
                             cap, test::builtin_registry());
  CHECK_EQUAL(plan.query, "T");
  plan = plan_kql_query("T", {}, paths({"this"}), None{}, cap,
                        test::builtin_registry());
  CHECK_EQUAL(plan.query, "T");
  plan = plan_kql_query("T", {}, ir::OptimizeProjection{}, None{}, cap,
                        test::builtin_registry());
  CHECK_EQUAL(plan.query, "T");
  plan = plan_kql_query("T", {}, paths({"this[\"a\\u{2028}b\"]"}), None{}, cap,
                        test::builtin_registry());
  CHECK_EQUAL(plan.query, "T\n| project ['a\\u2028b'] = "
                          "column_ifexists('a\\u2028b', dynamic(null))");
  CHECK(kql_accepts_trailing_stages(plan.query));
  plan = plan_kql_query("T", {}, paths({"this[\"it's\"]"}), None{}, cap,
                        test::builtin_registry());
  CHECK_EQUAL(plan.query, "T\n| project ['it\\'s'] = column_ifexists('it\\'s', "
                          "dynamic(null))");
}

TEST("kql plans leave unfit queries unchanged") {
  auto filter = filters({"x == 1"});
  for (auto query : {
         "T | render timechart",
         "T | fork (take 1) (take 2)",
         "T | facet by x",
         "T;",
         "T |",
         ".show tables",
         "T | where x == 'unterminated",
         "T | where x == \"line\nbreak\"",
         "T | where x == @'verbatim",
         "",
         "  // only a comment",
         // Kusto ends comments at CR, U+2028, and U+2029, too.
         "T // comment\r| render table",
         "T // comment\xE2\x80\xA8| render table",
         "T // comment\xE2\x80\xA9| render table",
         "T // comment\r;",
         // Single-line strings cannot contain line breaks.
         "T | where x == 'a\rb'",
         "T | where x == 'a\xE2\x80\xA8"
         "b'",
         "T | where x == @\"a\xE2\x80\xA9"
         "b\"",
       }) {
    CHECK(not kql_accepts_trailing_stages(query));
    CHECK_EQUAL(plan_kql_query(query, filter, None{}, None{}, cap,
                               test::builtin_registry())
                  .query,
                query);
  }
  for (auto query : {
         "T | where x == 'render'",
         "T | where x == @'fork''s'",
         "T | where x == h\"facet\"",
         "T // render",
         "T // render\r| take 1",
         "T // render\xE2\x80\xA8| take 1",
         "T | where x == 'a'\xE2\x80\xA9",
         "let a = 1; T | extend renderer = a",
         "T | where x == ```render\n;```",
         "T | where x == \"a\\\"; render\"",
       }) {
    CHECK(kql_accepts_trailing_stages(query));
  }
}
