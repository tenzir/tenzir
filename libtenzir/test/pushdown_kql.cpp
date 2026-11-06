//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/ip.hpp"
#include "tenzir/pushdown/kql.hpp"
#include "tenzir/pushdown/sql_source.hpp"
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
using namespace std::literals;

namespace {

using Path = std::vector<std::string>;

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
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  return std::move(*expr);
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

auto filter(std::string_view predicate) -> ir::QueryStage {
  return {ir::QueryStage::Filter{parse(predicate)}};
}

/// A filter with a constant that the optimizer folded, such as a list.
auto folded(std::string_view field, ast::binary_op op, data value)
  -> ir::QueryStage {
  return {ir::QueryStage::Filter{ast::binary_expr{
    parse(field), op,
    ast::constant::make(located{std::move(value), location::unknown})}}};
}

auto select(std::vector<std::pair<Path, Path>> fields) -> ir::QueryStage {
  auto result = ir::QueryStage::Select{};
  for (auto& [output, input] : fields) {
    result.fields.push_back({.output = output, .input = input});
  }
  return {std::move(result)};
}

auto drop(std::vector<Path> fields) -> ir::QueryStage {
  return {ir::QueryStage::Drop{std::move(fields)}};
}

auto key(Path input, Option<Path> output = None{}) -> ir::AggregateColumn {
  auto out = output ? std::move(*output) : input;
  return {.output = std::move(out), .function = None{}, .input = input};
}

auto call(Path output, std::string function, Option<Path> input = None{})
  -> ir::AggregateColumn {
  return {
    .output = std::move(output),
    .function = std::move(function),
    .input = std::move(input),
  };
}

auto aggregate(std::vector<ir::AggregateColumn> columns) -> ir::QueryStage {
  return {ir::QueryStage::Aggregate{.columns = std::move(columns),
                                    .schema_name = "tenzir.summarize"}};
}

auto sort(std::vector<std::string_view> keys, Option<uint64_t> bound = None{})
  -> ir::QueryStage {
  auto result = ir::QueryStage::Sort{.keys = {}, .bound = bound};
  for (auto key : keys) {
    auto descending = key.starts_with('-');
    if (descending) {
      key.remove_prefix(1);
    }
    result.keys.push_back(
      {.field = {std::string{key}}, .descending = descending});
  }
  return {std::move(result)};
}

auto limit(uint64_t count) -> ir::QueryStage {
  return {ir::QueryStage::Limit{count}};
}

/// The text that a source appends to the query `T` for `stages`, or `"-"` if
/// KQL does not spell them.
auto kql(std::vector<ir::QueryStage> stages,
         Option<std::vector<std::string_view>> projection = None{})
  -> std::string {
  auto projected = Option<ir::OptimizeProjection>{};
  if (projection) {
    projected = paths(*projection);
  }
  auto query = plan_kql_query("T", stages, projected);
  if (not query) {
    return "-";
  }
  CHECK_EQUAL(query->query, "T" + query->generated);
  return query->generated;
}

/// The predicate of the `where` stage of a filter, or the empty string if KQL
/// does not spell it.
auto where(std::string_view predicate) -> std::string {
  auto text = kql({filter(predicate)});
  if (text == "-") {
    return {};
  }
  constexpr auto prefix = "\n| where "sv;
  REQUIRE(text.starts_with(prefix));
  return text.substr(prefix.size());
}

} // namespace

TEST("kql spells literals") {
  CHECK_EQUAL(where(R"(x == "a\"b\\c")"), R"((['x'] == "a\"b\\c"))");
  CHECK_EQUAL(where(R"(x == "Straße")"), R"((['x'] == "Straße"))");
  // Control characters use KQL's escapes, and Unicode line separators are
  // escaped, so that the literal stays on one line.
  CHECK_EQUAL(where("x == \"a\\nb\\t\\r\\u{1}\\u{7f}\""),
              R"((['x'] == "a\nb\t\r\u0001\u007f"))");
  CHECK_EQUAL(where("x == \"a\\u{2028}b\\u{2029}c\\u{85}d\""),
              R"((['x'] == "a\u2028b\u2029c\u0085d"))");
  CHECK_EQUAL(where("x == 42"), "(['x'] == 42)");
  CHECK_EQUAL(where("x == 9223372036854775807"),
              "(['x'] == 9223372036854775807)");
  CHECK_EQUAL(where("x == 18446744073709551615"), "");
  CHECK_EQUAL(where("x == 1.5"), "(['x'] == 1.5)");
  // A `double` keeps a `real` spelling.
  CHECK_EQUAL(where("x == 2.0"), "(['x'] == 2.0)");
  CHECK_EQUAL(where("x / 2.0 == 1"), "((['x'] / 2.0) == 1)");
  CHECK_EQUAL(where("x / 2 == 1"), "((['x'] / 2) == 1)");
  CHECK_EQUAL(where("x > 1e300"), "(['x'] > 1e+300)");
  CHECK_EQUAL(where("x > 0.00000015"), "(['x'] > 1.5e-07)");
  CHECK_EQUAL(where("x == true"), "(['x'] == true)");
  CHECK_EQUAL(where("x == 2026-10-19T10:03:00.1234567Z"),
              "(['x'] == datetime(2026-10-19T10:03:00.1234567Z))");
  CHECK_EQUAL(where("x < 1969-12-31T23:59:59.9999999Z"),
              "(['x'] < datetime(1969-12-31T23:59:59.9999999Z))");
  CHECK_EQUAL(where("x > 2026-10-19"),
              "(['x'] > datetime(2026-10-19T00:00:00.0000000Z))");
  // The earliest times spell, too, although the start of their day is out of
  // range in nanoseconds.
  CHECK_EQUAL(
    kql({folded("x", ast::binary_op::gt,
                data{tenzir::time{duration{-9'223'369'200'000'000'000}}})}),
    "\n| where (['x'] > datetime(1677-09-21T01:00:00.0000000Z))");
  CHECK_EQUAL(
    kql({folded("x", ast::binary_op::gt,
                data{tenzir::time{duration{9'223'372'036'854'775'800}}})}),
    "\n| where (['x'] > datetime(2262-04-11T23:47:16.8547758Z))");
  // KQL holds times and durations in ticks of 100 nanoseconds.
  CHECK_EQUAL(where("x > 2026-10-19T10:03:00.123456789Z"), "");
  CHECK_EQUAL(where("x > 1h"), "(['x'] > timespan(36000000000tick))");
  CHECK_EQUAL(where("x > 100ns"), "(['x'] > timespan(1tick))");
  CHECK_EQUAL(where("x > 1ns"), "");
  CHECK_EQUAL(where("x - 2026-01-01 > 1d"),
              "((['x'] - datetime(2026-01-01T00:00:00.0000000Z)) > "
              "timespan(864000000000tick))");
  // The smallest `long` has no literal of its own.
  CHECK_EQUAL(kql({folded("x", ast::binary_op::gt,
                          data{std::numeric_limits<int64_t>::min()})}),
              "\n| where (['x'] > long(-9223372036854775808))");
  // KQL has no `null` literal that compares with every type.
  CHECK_EQUAL(where("x > null"), "");
}

TEST("kql spells expressions") {
  CHECK_EQUAL(where("x != 1"), "(['x'] != 1)");
  CHECK_EQUAL(where("x == null"), "isnull(['x'])");
  CHECK_EQUAL(where("null != x"), "isnotnull(['x'])");
  CHECK_EQUAL(where("x in [1, 2]"), "(['x'] in (1, 2))");
  CHECK_EQUAL(where("x not in [1, 2]"), "not((['x'] in (1, 2)))");
  CHECK_EQUAL(where("x in []"), "");
  CHECK_EQUAL(where("x > 1 and (y < 2 or not z)"),
              "((['x'] > 1) and ((['y'] < 2) or not(['z'])))");
  CHECK_EQUAL(where("flag"), "['flag']");
  CHECK_EQUAL(where("-x + 1 * y - 2 > 0"),
              "((((-['x']) + (1 * ['y'])) - 2) > 0)");
  CHECK_EQUAL(where(R"(x.starts_with("a"))"), R"((['x'] startswith_cs "a"))");
  CHECK_EQUAL(where(R"(x.ends_with("a"))"), R"((['x'] endswith_cs "a"))");
  CHECK_EQUAL(where(R"("a" in x)"), R"((['x'] contains_cs "a"))");
  CHECK_EQUAL(where(R"(x.match_regex("^a"))"), R"((['x'] matches regex "^a"))");
  CHECK_EQUAL(where(R"(x.to_lower() == "a")"), R"((tolower(['x']) == "a"))");
  CHECK_EQUAL(where(R"(x.to_upper() == "A")"), R"((toupper(['x']) == "A"))");
  CHECK_EQUAL(where("x.length_chars() > 1"), "(strlen(['x']) > 1)");
  CHECK_EQUAL(where("x.abs() > 1"), "(abs(['x']) > 1)");
  CHECK_EQUAL(where(R"(x.trim() == "a")"), R"((trim(@"\s+", ['x']) == "a"))");
  // KQL rejects an ordering of strings.
  CHECK_EQUAL(where(R"(x < "b")"), "");
  CHECK_EQUAL(where(R"("b" >= x)"), "");
  // Names spell with escapes. A `*` in a name is a wildcard in a command, and
  // nested fields bind to no column.
  CHECK_EQUAL(where(R"(this["it's \\ a.b"] == 1)"),
              R"((['it\'s \\ a.b'] == 1))");
  CHECK_EQUAL(where(R"(this["a*"] == 1)"), "");
  CHECK_EQUAL(where("x.y == 1"), "");
  // Optional fields, named arguments, and other functions have no spelling.
  CHECK_EQUAL(where("x? == 1"), "");
  CHECK_EQUAL(where(R"(x.starts_with("a", ignore_case=true))"), "");
  CHECK_EQUAL(where("x.length_bytes() > 1"), "");
}

TEST("kql spells IP addresses and subnets as strings") {
  CHECK_EQUAL(where("x == 10.0.0.1"), R"((['x'] == "10.0.0.1"))");
  CHECK_EQUAL(where("::1 != x"), R"(("::1" != ['x']))");
  CHECK_EQUAL(where("x in [10.0.0.1, 10.0.0.2]"),
              R"((['x'] in ("10.0.0.1", "10.0.0.2")))");
  CHECK_EQUAL(kql({folded("x", ast::binary_op::in,
                          data{list{data{ip::v4(uint32_t{1})}}})}),
              "\n| where (['x'] in (\"0.0.0.1\"))");
  CHECK_EQUAL(where("x in 10.0.0.0/8"),
              R"(ipv4_is_in_range(['x'], "10.0.0.0/8"))");
  CHECK_EQUAL(where("x in 0.0.0.0/0"),
              R"(ipv4_is_in_range(['x'], "0.0.0.0/0"))");
  CHECK_EQUAL(where("x in fe80::/10"),
              R"(ipv6_is_in_range(['x'], "fe80::/10"))");
  // KQL may read IPv4 addresses as mapped IPv6 addresses.
  CHECK_EQUAL(where("x in ::/0"), "");
  CHECK_EQUAL(where("x in ::ffff:0:0/95"), "");
  // Addresses neither order nor compute.
  CHECK_EQUAL(where("x < 10.0.0.1"), "");
  CHECK_EQUAL(where("x == 10.0.0.0/8"), "");
}

TEST("kql spells selections with project and renames in the layout") {
  auto stages = std::vector{
    select({{{"u"}, {"user"}}, {{"n"}, {"n"}}, {{"x", "y"}, {"n"}}})};
  auto query = plan_kql_query("T", stages, None{});
  REQUIRE(query);
  CHECK_EQUAL(query->generated, "\n| project ['user'], ['n']");
  REQUIRE(query->layout.fields);
  auto const expected = LayoutFields{
    {{"u"}, "user"},
    {{"n"}, "n"},
    {{"x", "y"}, "n"},
  };
  CHECK_EQUAL(*query->layout.fields, expected);
  // A swap renames in the layout, and stages after it read the columns
  // that hold the fields.
  CHECK_EQUAL(kql({select({{{"b"}, {"a"}}, {{"a"}, {"b"}}}), filter("a > 1")}),
              "\n| project ['a'], ['b']\n| where (['b'] > 1)");
  // Fields that the selection removed are gone, and nested inputs bind to no
  // column.
  CHECK_EQUAL(kql({select({{{"a"}, {"a"}}}), filter("b > 1")}), "-");
  CHECK_EQUAL(kql({select({{{"a"}, {"x", "y"}}})}), "-");
  CHECK_EQUAL(kql({select({{{"a"}, {"a*"}}})}), "-");
  CHECK_EQUAL(kql({select({})}), "-");
}

TEST("kql drops columns with project-away") {
  CHECK_EQUAL(kql({drop({{"a"}, {"b c"}})}), "\n| project-away ['a'], ['b c']");
  CHECK_EQUAL(kql({drop({{"a"}}), filter("a > 1")}), "-");
  CHECK_EQUAL(kql({drop({{"a", "b"}})}), "-");
  // `project-away` reads a `*` as a wildcard: `Na*` would drop `Name`.
  CHECK_EQUAL(kql({drop({{"Na*"}})}), "-");
  // After a selection, a drop only changes the layout.
  auto query = plan_kql_query(
    "T", std::vector{select({{{"a"}, {"a"}}, {{"b"}, {"b"}}}), drop({{"a"}})},
    None{});
  REQUIRE(query);
  CHECK_EQUAL(query->generated, "\n| project ['a'], ['b']");
  CHECK_EQUAL(*query->layout.fields, (LayoutFields{{{"b"}, "b"}}));
}

TEST("kql spells summaries with summarize") {
  auto stages = std::vector{aggregate({
    key({"k"}),
    call({"n"}, "count"),
    call({"c"}, "count", Path{"x"}),
    call({"lo"}, "min", Path{"x"}),
    call({"hi"}, "max", Path{"x"}),
    call({"s"}, "sum", Path{"x"}),
    call({"again"}, "count"),
    key({"j"}, Path{"out", "j"}),
  })};
  auto query = plan_kql_query("T", stages, None{});
  REQUIRE(query);
  CHECK_EQUAL(query->generated,
              "\n| summarize ['__tenzir_column_0'] = count(), "
              "['__tenzir_column_1'] = countif(isnotempty(['x'])), "
              "['__tenzir_column_2'] = min(['x']), ['__tenzir_column_3'] = "
              "max(['x']), ['__tenzir_column_4'] = sum(['x']) by ['k'], ['j']");
  auto const expected = LayoutFields{
    {{"k"}, "k"},
    {{"n"}, "__tenzir_column_0"},
    {{"c"}, "__tenzir_column_1"},
    {{"lo"}, "__tenzir_column_2"},
    {{"hi"}, "__tenzir_column_3"},
    {{"s"}, "__tenzir_column_4"},
    {{"again"}, "__tenzir_column_0"},
    {{"out", "j"}, "j"},
  };
  REQUIRE(query->layout.fields);
  CHECK_EQUAL(*query->layout.fields, expected);
  CHECK_EQUAL(query->layout.schema_name,
              Option<std::string>{"tenzir.summarize"});
  // Group keys alone still need a function, which the layout leaves out.
  CHECK_EQUAL(kql({aggregate({key({"k"})})}),
              "\n| summarize ['__tenzir_count_0'] = count() by ['k']");
  // Stages after a summary read its columns.
  CHECK_EQUAL(kql({filter("x > 1"),
                   aggregate({key({"k"}), call({"n"}, "count")}),
                   filter("n > 2"), sort({"-n", "k"}, 3), limit(3)}),
              "\n| where (['x'] > 1)\n| summarize ['__tenzir_column_0'] = "
              "count() by ['k']\n| where (['__tenzir_column_0'] > 2)\n| sort "
              "by ['__tenzir_column_0'] desc, ['k'] asc\n| take 3");
  // Generated names avoid the names that the stages reference.
  CHECK_EQUAL(kql({aggregate({key({"__tenzir_column_0"}), call({"n"}, "coun"
                                                                      "t")})}),
              "\n| summarize ['__tenzir_column_1'] = count() by "
              "['__tenzir_column_0']");
  // `avg` yields `NaN` for no values, and other functions have no spelling.
  CHECK_EQUAL(kql({aggregate({call({"m"}, "mean", Path{"x"})})}), "-");
  CHECK_EQUAL(kql({aggregate({call({"m"}, "count_distinct", Path{"x"})})}),
              "-");
  CHECK_EQUAL(kql({aggregate({call({"m"}, "min")})}), "-");
  CHECK_EQUAL(kql({aggregate({key({"x", "y"})})}), "-");
  CHECK_EQUAL(kql({aggregate({key({"a*"}), call({"n"}, "count")})}), "-");
}

TEST("kql sorts with explicit directions and limits with take") {
  CHECK_EQUAL(kql({sort({"-n", "user"})}),
              "\n| sort by ['n'] desc, ['user'] asc");
  CHECK_EQUAL(kql({sort({"n"}, 5), limit(5)}),
              "\n| sort by ['n'] asc\n| take 5");
  CHECK_EQUAL(kql({sort({"a*"})}), "-");
  CHECK_EQUAL(kql({limit(0)}), "\n| take 0");
  CHECK_EQUAL(kql({limit(9223372036854775807)}),
              "\n| take 9223372036854775807");
  CHECK_EQUAL(kql({limit(9223372036854775808u)}), "-");
  CHECK_EQUAL(kql({limit(5), filter("x > 1")}),
              "\n| take 5\n| where (['x'] > 1)");
}

TEST("kql keeps the fields that downstream needs") {
  CHECK_EQUAL(kql({}, std::vector{"a"sv, "b.c"sv, "a"sv}),
              "\n| project ['a'] = column_ifexists('a', dynamic(null)), ['b'] "
              "= column_ifexists('b', dynamic(null))");
  CHECK_EQUAL(kql({}, std::vector{R"(this["it's"])"sv}),
              "\n| project ['it\\'s'] = column_ifexists('it\\'s', "
              "dynamic(null))");
  CHECK_EQUAL(kql({filter("x > 1")}, std::vector{"a"sv}),
              "\n| where (['x'] > 1)\n| project ['a'] = column_ifexists('a', "
              "dynamic(null))");
  // A projection of whole events, of nothing, or with a name that has no
  // spelling keeps every column.
  CHECK_EQUAL(kql({}, std::vector{"this"sv}), "");
  CHECK_EQUAL(kql({}, std::vector<std::string_view>{}), "");
  CHECK_EQUAL(kql({}, std::vector{"a"sv, R"(this["b*"])"sv}), "");
  // A drop leaves its fields out, and a selection or a summary keeps its own
  // fields.
  CHECK_EQUAL(kql({drop({{"a"}})}, std::vector{"a"sv, "b"sv}),
              "\n| project-away ['a']\n| project ['b'] = column_ifexists('b', "
              "dynamic(null))");
  CHECK_EQUAL(kql({select({{{"a"}, {"a"}}})}, std::vector{"a"sv}),
              "\n| project ['a']");
}

TEST("kql appends at most a bounded number of bytes") {
  // A filter whose spelling exceeds the budget stays in the pipeline.
  auto values = std::vector<std::string>{};
  for (auto i = 0; i < 2'000; ++i) {
    values.push_back(fmt::format("\"value-{:05}\"", i));
  }
  auto const large = fmt::format("x in [{}]", fmt::join(values, ", "));
  CHECK_EQUAL(kql({filter(large)}), "-");
  CHECK(not plan_kql_query("T", std::vector{filter(large)}, None{}));
  // One that fits still goes out.
  auto const small
    = fmt::format("x in [{}]", fmt::join(std::span{values}.first(10), ", "));
  CHECK(kql({filter(small)}) != "-");
  // A projection that would exceed the budget stays out of the query, while
  // the stages before it still go out.
  auto fields = std::vector<std::string>{};
  for (auto i = 0; i < 1'000; ++i) {
    fields.push_back(fmt::format("field_{:04}", i));
  }
  auto names = std::vector<std::string_view>{fields.begin(), fields.end()};
  CHECK_EQUAL(kql({}, names), "");
  CHECK_EQUAL(kql({filter("x > 1")}, names), "\n| where (['x'] > 1)");
  auto const fitting
    = std::vector<std::string_view>{names.begin(), names.begin() + 3};
  CHECK(kql({}, fitting).starts_with("\n| project"));
}

TEST("kql appends stages only to queries that accept them") {
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
         "set notruncation;\nT",
         // Every tabular statement returns a table, and appended stages
         // apply to the last one only.
         "print x=1; print x=2",
         "T;\nU",
         "let a = 1;; T",
         ";T",
         // A table may be named like a declaration keyword, which then
         // returns a table of its own.
         "let alias = datatable(x: long)[1]; alias; print x=2",
         "let let = datatable(x: long)[1]; let; print x=2",
         "let declare = datatable(x: long)[1]; declare; print x=2",
         "let as = T; let as x; U",
         "declare | take 1; T",
         "alias | take 1; T",
         "let == 1; T",
         "let ['a'] = 1; T",
         // Brackets must balance.
         "T | where (x == 1",
         "T | where x == 1)",
       }) {
    CHECK(not kql_accepts_trailing_stages(query));
    auto unchanged = plan_kql_query(query, {}, paths({"a"}));
    REQUIRE(unchanged);
    CHECK_EQUAL(unchanged->query, query);
    CHECK_EQUAL(unchanged->generated, "");
    CHECK(not plan_kql_query(query, std::vector{filter("x == 1")}, None{}));
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
         "let a = 1;\nlet f = (x: long) { T | where y > x; };\nf(a)",
         "declare query_parameters(p: long = 1);\nT | where x == p",
         "alias database db = cluster('c').database('d');\nT",
         "let t = materialize(T);\nunion (t | take 1), (t | take 2)",
         "T | where x == ';'",
         "T // ;\n| take 1",
       }) {
    CHECK(kql_accepts_trailing_stages(query));
  }
  // A stage starts on a new line, which ends a trailing comment.
  auto query = plan_kql_query("T // note", std::vector{limit(1)}, None{});
  REQUIRE(query);
  CHECK_EQUAL(query->query, "T // note\n| take 1");
  CHECK(kql_accepts_trailing_stages(query->query));
  // A planned query accepts further stages, also with escaped separators.
  query = plan_kql_query(
    "T", std::vector{filter("x == \"a\\u{2028}b\\u{2029}c\\u{85}d\"")}, None{});
  REQUIRE(query);
  CHECK(kql_accepts_trailing_stages(query->query));
}

TEST("kql sources take over the longest prefix that KQL spells") {
  auto const absorb
    = [](std::string_view query, std::vector<ir::QueryStage> stages) {
        return absorb_prefix(
          ir::QueryChain{.stages = std::move(stages), .end = ""},
          [&](std::span<ir::QueryStage const> prefix) {
            return plan_kql_query(query, prefix, None{}).is_some();
          },
          {.target = "Defender", .filters_only = false, .veto = {}});
      };
  auto all = absorb("T", {filter("x > 1"),
                          aggregate({key({"k"}), call({"n"}, "count")}),
                          sort({"-n"}), limit(3)});
  CHECK_EQUAL(all.prefix, size_t{4});
  auto partial = absorb("T", {filter("x > 1"), filter(R"(x < "b")"), limit(3)});
  CHECK_EQUAL(partial.prefix, size_t{1});
  CHECK_EQUAL(partial.declined, "Defender cannot spell the stage"s);
  auto none = absorb("print x=1; print x=2", {filter("x > 1")});
  CHECK_EQUAL(none.prefix, size_t{0});
}
