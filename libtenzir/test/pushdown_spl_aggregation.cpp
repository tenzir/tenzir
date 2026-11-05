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

auto filter(std::string_view predicate) -> ir::QueryStage {
  return {ir::QueryStage::Filter{parse(predicate)}};
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
  return plan_spl_query(source, stages, None{});
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
  CHECK_EQUAL(query->counts,
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
  CHECK_EQUAL(query->counts, std::vector<std::string>{"tenzir_column_0"});
}

TEST("summaries of group keys only count in a hidden column") {
  auto query = plan({aggregate({key({"user"}), key({"a b"}, Path{"x", "y"})})});
  REQUIRE(query);
  CHECK_EQUAL(query->search,
              "search index=\"main\"\n| stats count as tenzir_count_0 by "
              "user, \"a b\"\n| appendpipe [stats count as tenzir_groups_0]");
  CHECK_EQUAL(*query->layout.fields,
              (LayoutFields{{{"user"}, "user"}, {{"x", "y"}, "a b"}}));
  CHECK(query->counts.empty());
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
  CHECK_EQUAL(query->counts, std::vector<std::string>{"tenzir_column_0"});
  // The window ends at the summary.
  auto windowed = plan({
    aggregate({key({"_time"})}),
    filter("_time > 2025-01-01T00:00:00Z"),
  });
  REQUIRE(windowed);
  CHECK(not windowed->window.begin);
}

TEST("summaries with functions that Splunk does not compute stay local") {
  CHECK(not plan({aggregate({call({"s"}, "sum", Path{"n"})})}));
  CHECK(not plan({aggregate({call({"s"}, "count_distinct", Path{"n"})})}));
  CHECK(not plan({aggregate({call({"s"}, "min")})}));
  CHECK(not plan({aggregate({key({"a*"}), call({"c"}, "count")})}));
  CHECK(not plan({aggregate({key({"a", "b"}), call({"c"}, "count")})}));
}
