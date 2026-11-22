//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/splunk.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::pushdown;
using namespace std::literals;

namespace {

/// The result columns of `layout` that decode as `decode`.
auto decoded(Layout const& layout, Decode decode) -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  for (auto const& [column, x] : layout.decode) {
    if (x == decode) {
      result.push_back(column);
    }
  }
  return result;
}

using Path = std::vector<std::string>;

/// The field at `path` as an expression.
auto field_at(std::vector<std::string> const& path) -> ast::expression {
  auto result = ast::expression{
    ast::root_field{ast::identifier{path.front(), location::unknown}, false}};
  for (auto i = size_t{1}; i < path.size(); ++i) {
    result = ast::field_access{std::move(result), location::unknown, false,
                               ast::identifier{path[i], location::unknown}};
  }
  return result;
}

/// The arguments of a column that reads `input`, if any.
auto args_of(Option<std::vector<std::string>> const& input)
  -> std::vector<ast::expression> {
  if (not input) {
    return {};
  }
  return {field_at(*input)};
}

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

auto filter(std::string_view predicate) -> ir::QueryStage {
  return {ir::QueryStage::Filter{parse(predicate)}};
}

auto key(Path input, Option<Path> output = None{}) -> ir::AggregateColumn {
  auto out = output ? std::move(*output) : input;
  return {
    .output = std::move(out), .function = None{}, .args = {field_at(input)}};
}

auto call(Path output, std::string function, Option<Path> input = None{})
  -> ir::AggregateColumn {
  return {
    .output = std::move(output),
    .function = std::move(function),
    .args = args_of(input),
  };
}

auto aggregate(std::vector<ir::AggregateColumn> columns) -> ir::QueryStage {
  return {ir::QueryStage::Aggregate{.columns = std::move(columns),
                                    .schema_name = "tenzir.summarize"}};
}

auto select(std::vector<std::pair<Path, Path>> fields) -> ir::QueryStage {
  auto result = ir::QueryStage::Select{};
  for (auto& [output, input] : fields) {
    result.fields.push_back({.output = output, .input = input});
  }
  return {std::move(result)};
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

auto main_index() -> SplSource {
  return {.search = None{}, .indexes = {"main"}};
}

auto plan(std::vector<ir::QueryStage> stages,
          SplSource const& source = main_index()) -> Option<SplQuery> {
  auto planned = spl_planner(source, None{}, None{}).plan(stages, nullptr);
  if (not planned) {
    return None{};
  }
  return std::move(planned).unwrap();
}

/// The summary of the mock and live tests.
auto by_user() -> ir::QueryStage {
  return aggregate({
    key({"user"}),
    call({"events"}, "count"),
    call({"actions"}, "count", Path{"action"}),
    call({"lo"}, "min", Path{"n"}),
    call({"hi"}, "max", Path{"n"}),
  });
}

} // namespace

TEST("summaries become stats stages") {
  auto query = plan({by_user()});
  REQUIRE(query);
  CHECK_EQUAL(query->search,
              "search index=\"main\"\n| stats count as tenzir_column_0, "
              "count(action) as tenzir_column_1, min(n) as tenzir_column_2, "
              "max(n) as tenzir_column_3 by user\n| appendpipe [stats count "
              "as tenzir_groups_0]");
  REQUIRE(query->layout.fields);
  auto const expected = LayoutFields{
    {{"user"}, "user"},
    {{"events"}, "tenzir_column_0"},
    {{"actions"}, "tenzir_column_1"},
    {{"lo"}, "tenzir_column_2"},
    {{"hi"}, "tenzir_column_3"},
  };
  CHECK_EQUAL(*query->layout.fields, expected);
  REQUIRE(query->layout.schema_name);
  CHECK_EQUAL(*query->layout.schema_name, "tenzir.summarize"s);
  REQUIRE(query->sentinel);
  CHECK_EQUAL(*query->sentinel, "tenzir_groups_0"s);
  CHECK_EQUAL(decoded(query->layout, Decode::integer),
              (std::vector<std::string>{"tenzir_column_0", "tenzir_column_1"}));
  // The search that a user wrote gets the same stages.
  auto search = plan({by_user()}, {.search = "index=main", .indexes = {}});
  REQUIRE(search);
  CHECK(search->search.starts_with("search index=main\n| stats count as "));
  CHECK(search->generated.starts_with("\n| stats count as "));
}

TEST("summaries compute repeated calls and keys once") {
  // Splunk computes a repeated call once and keeps only its last name.
  auto query = plan({aggregate({
    call({"a"}, "count"),
    call({"b"}, "count"),
    call({"c"}, "min", Path{"n"}),
    call({"d"}, "min", Path{"n"}),
    key({"k"}),
    key({"k"}, Path{"k2"}),
  })});
  REQUIRE(query);
  CHECK_EQUAL(query->search,
              "search index=\"main\"\n| stats count as tenzir_column_0, "
              "min(n) as tenzir_column_1 by k\n| appendpipe [stats count as "
              "tenzir_groups_0]");
  auto const expected = LayoutFields{
    {{"a"}, "tenzir_column_0"},
    {{"b"}, "tenzir_column_0"},
    {{"c"}, "tenzir_column_1"},
    {{"d"}, "tenzir_column_1"},
    {{"k"}, "k"},
    {{"k2"}, "k"},
  };
  CHECK_EQUAL(*query->layout.fields, expected);
  CHECK_EQUAL(decoded(query->layout, Decode::integer),
              std::vector<std::string>{"tenzir_column_0"});
}

TEST("summaries of group keys only count in a hidden column") {
  auto query = plan({aggregate({key({"user"}), key({"a b"}, Path{"x", "y"})})});
  REQUIRE(query);
  CHECK_EQUAL(query->search,
              "search index=\"main\"\n| stats count as tenzir_count_0 by "
              "user, \"a b\"\n| appendpipe [stats count as tenzir_groups_0]");
  CHECK_EQUAL(*query->layout.fields,
              (LayoutFields{{{"user"}, "user"}, {{"x", "y"}, "a b"}}));
  CHECK(decoded(query->layout, Decode::integer).empty());
  // A summary without columns yields one event without fields.
  auto empty = plan({aggregate({})});
  REQUIRE(empty);
  CHECK_EQUAL(empty->search, "search index=\"main\"\n| stats count as "
                             "tenzir_count_0\n| appendpipe [stats count as "
                             "tenzir_groups_0]");
  CHECK(empty->layout.fields);
  CHECK(empty->layout.fields->empty());
}

TEST("summaries avoid the names that the stages reference") {
  auto query = plan({aggregate({
    call({"tenzir_column_0"}, "count"),
    call({"tenzir_groups_0"}, "max", Path{"tenzir_column_1"}),
  })});
  REQUIRE(query);
  CHECK_EQUAL(query->search,
              "search index=\"main\"\n| stats count as tenzir_column_2, "
              "max(tenzir_column_1) as tenzir_column_3\n| appendpipe [stats "
              "count as tenzir_groups_1]");
  REQUIRE(query->sentinel);
  CHECK_EQUAL(*query->sentinel, "tenzir_groups_1"s);
}

TEST("stages after a summary read its columns") {
  auto query = plan({
    filter(R"(source == "auth" and user != "root")"),
    aggregate({key({"user"}), call({"events"}, "count")}),
    filter("events > 1"),
    sort({"-events"}, 3),
    limit(3),
    select({{{"who"}, {"user"}}, {{"n"}, {"events"}}}),
  });
  REQUIRE(query);
  CHECK_EQUAL(query->search,
              "search index=\"main\" source=\"auth\"\n| where ('user' != "
              "\"root\")\n| stats count as tenzir_column_0 by user\n| where "
              "('tenzir_column_0' > 1)\n| sort 3 -tenzir_column_0\n| head "
              "3\n| fields user, tenzir_column_0\n| appendpipe [stats count "
              "as tenzir_groups_0]");
  CHECK_EQUAL(*query->layout.fields,
              (LayoutFields{{{"who"}, "user"}, {{"n"}, "tenzir_column_0"}}));
  REQUIRE(query->layout.schema_name);
  CHECK_EQUAL(*query->layout.schema_name, "tenzir.summarize"s);
  CHECK_EQUAL(decoded(query->layout, Decode::integer),
              std::vector<std::string>{"tenzir_column_0"});
  // The window ends at the summary.
  auto windowed = plan({
    aggregate({key({"_time"})}),
    filter("_time > 2025-01-01T00:00:00Z"),
  });
  REQUIRE(windowed);
  CHECK(not windowed->window.begin);
}

TEST("summaries count conditionally and distinctly, sum, and average") {
  auto query = plan({aggregate({
    call({"big"}, "count_if", Path{"n"}),
    call({"users"}, "count_distinct", Path{"user"}),
    call({"total"}, "sum", Path{"n"}),
    call({"avg"}, "mean", Path{"n"}),
  })});
  // `count_if` without its predicate has no spelling.
  CHECK(not query);
  auto big = by_user();
  auto& columns = as<ir::QueryStage::Aggregate>(big.kind).columns;
  columns = {
    {.output = {"big"},
     .function = "count_if",
     .args = {field_at({"n"}), parse("n > 1")}},
    call({"users"}, "count_distinct", Path{"user"}),
    call({"total"}, "sum", Path{"n"}),
    call({"avg"}, "mean", Path{"n"}),
  };
  query = plan({big});
  REQUIRE(query);
  CHECK_EQUAL(query->search,
              "search index=\"main\"\n| stats count(eval(('n' > 1))) as "
              "tenzir_column_0, dc(user) as tenzir_column_1, sum(n) as "
              "tenzir_column_2, avg(n) as tenzir_column_3\n| appendpipe "
              "[stats count as tenzir_groups_0]");
  CHECK_EQUAL(decoded(query->layout, Decode::integer),
              (std::vector<std::string>{"tenzir_column_0", "tenzir_column_1"}));
  CHECK_EQUAL(decoded(query->layout, Decode::number),
              (std::vector<std::string>{"tenzir_column_2", "tenzir_column_3"}));
  // A predicate compares no time literal, which only `_time` compares with.
  columns.resize(1);
  columns[0].args[1] = parse("n > 2024-01-01");
  CHECK(not plan({big}));
}

TEST("summaries compute keys and arguments with eval") {
  auto query = plan({aggregate({
    {.output = {"k"}, .function = None{}, .args = {parse("user.to_lower()")}},
    {.output = {"m"}, .function = "max", .args = {parse("n * 2")}},
    {.output = {"again"}, .function = "min", .args = {parse("n * 2")}},
  })});
  REQUIRE(query);
  CHECK_EQUAL(query->search,
              "search index=\"main\"\n| eval tenzir_column_0=lower('user'), "
              "tenzir_column_1=('n' * 2)\n| stats max(tenzir_column_1) as "
              "tenzir_column_2, min(tenzir_column_1) as tenzir_column_3 by "
              "tenzir_column_0\n| appendpipe [stats count as tenzir_groups_0]");
  // Time literals compare only with `_time` in a filter.
  CHECK(not plan({aggregate({{.output = {"k"},
                              .function = None{},
                              .args = {parse("_time - 2024-01-01")}}})}));
}

TEST("summaries spell all, any, predicate keys, and time functions") {
  auto query = plan({aggregate({
    {.output = {"sw"},
     .function = None{},
     .args = {parse(R"(user.starts_with("a"))")}},
    {.output = {"yr"}, .function = None{}, .args = {parse("_time.year()")}},
    {.output = {"bkt"}, .function = None{}, .args = {parse("floor(_time, 1h)")}},
    {.output = {"a"}, .function = "all", .args = {parse(R"(this["a.b"])")}},
    {.output = {"b"}, .function = "any", .args = {parse("flag")}},
  })});
  REQUIRE(query);
  CHECK_EQUAL(
    query->search,
    "search index=\"main\"\n| eval tenzir_column_0=if((substr('user', 1, "
    "len(\"a\"))==\"a\"), \"true\", \"false\"), "
    "tenzir_column_1=tonumber(strftime('_time', \"%Y\")), "
    "tenzir_column_2=strftime(floor('_time' / 3600) * 3600, "
    "\"%Y-%m-%dT%H:%M:%S%z\")\n| stats min(eval(case('a.b'==\"true\", 1, "
    "'a.b'==\"false\", 0))) as tenzir_column_3, "
    "max(eval(case('flag'==\"true\", "
    "1, 'flag'==\"false\", 0))) as tenzir_column_4 by tenzir_column_0, "
    "tenzir_column_1, tenzir_column_2\n| appendpipe [stats count as "
    "tenzir_groups_0]");
  CHECK_EQUAL(decoded(query->layout, Decode::boolean),
              (std::vector<std::string>{"tenzir_column_0", "tenzir_column_3",
                                        "tenzir_column_4"}));
  CHECK_EQUAL(decoded(query->layout, Decode::number),
              (std::vector<std::string>{"tenzir_column_1"}));
  CHECK_EQUAL(decoded(query->layout, Decode::time),
              (std::vector<std::string>{"tenzir_column_2"}));
  // SPL2 omits `eval(...)` inside `stats`.
  auto spl2
    = spl_planner(SplSource{.search = std::string{"from main"},
                            .indexes = {},
                            .spl2 = true},
                  None{}, None{})
        .plan(std::vector{aggregate({
                {.output = {"a"}, .function = "all", .args = {parse("flag")}},
                {.output = {"b"}, .function = "any", .args = {parse("flag")}},
              })},
              nullptr);
  REQUIRE(spl2);
  CHECK_EQUAL(
    spl2.unwrap().search,
    "from main\n| stats min(case('flag'==\"true\", 1, 'flag'==\"false\", 0)) "
    "as tenzir_column_0, max(case('flag'==\"true\", 1, 'flag'==\"false\", 0)) "
    "as tenzir_column_1\n| appendpipe [stats count() as tenzir_groups_0]");
}

TEST("summaries with functions that Splunk does not compute stay local") {
  CHECK(not plan({aggregate({call({"s"}, "median", Path{"n"})})}));
  CHECK(not plan({aggregate({call({"s"}, "min")})}));
  CHECK(not plan({aggregate({key({"a*"}), call({"c"}, "count")})}));
  CHECK(not plan({aggregate({key({"a", "b"}), call({"c"}, "count")})}));
}
