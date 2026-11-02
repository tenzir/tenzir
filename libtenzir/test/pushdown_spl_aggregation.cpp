//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/json_parser.hpp"
#include "tenzir/nova/materialize.hpp"
#include "tenzir/pushdown/splunk.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <fmt/format.h>

#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::pushdown;

namespace {

using Column = ir::OptimizeAggregation::Column;
using Path = std::vector<std::string>;

auto key(Path input, Option<Path> output = None{}) -> Column {
  auto out = output ? std::move(*output) : input;
  return {.output = std::move(out), .function = None{}, .input = input};
}

auto call(Path output, std::string function, Option<Path> input = None{})
  -> Column {
  return {
    .output = std::move(output),
    .function = std::move(function),
    .input = std::move(input),
  };
}

auto make(std::vector<Column> columns) -> ir::OptimizeAggregation {
  return {
    .columns = std::move(columns),
    .schema_name = "tenzir.summarize",
    .fallback = {},
  };
}

/// The search that computes `aggregation` after `search`, without a filter.
auto aggregate(std::string_view search,
               ir::OptimizeAggregation const& aggregation, bool raw = false)
  -> Option<std::string> {
  TRY(auto result, plan_spl_aggregation(search, {}, None{}, aggregation, raw,
                                        test::builtin_registry()));
  CHECK(not result.checked);
  return std::move(result.search);
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

/// The stages that `plan_spl_aggregation` appends to the search `S` for
/// `summarize n=count()` after the filters `predicates`.
auto filtered(std::vector<std::string_view> predicates, bool raw = false)
  -> Option<std::string> {
  auto filter = ir::OptimizeFilter{};
  for (auto predicate : predicates) {
    filter.push_back(parse(predicate));
  }
  auto planned
    = plan_spl_aggregation("S", filter, None{}, make({call({"n"}, "count")}),
                           raw, test::builtin_registry());
  if (not planned) {
    return None{};
  }
  // The search computes the check exactly if its filter stages assign it.
  CHECK_EQUAL(planned->checked,
              planned->search.contains("tenzir_filter_check"));
  auto result = Option<std::string>{std::move(planned->search)};
  constexpr auto suffix
    = std::string_view{"\n| stats count as tenzir_count\n"
                       "| appendpipe [stats count as tenzir_groups]"};
  REQUIRE(result->starts_with("S\n"));
  if (result->ends_with(suffix)) {
    return result->substr(2, result->size() - 2 - suffix.size());
  }
  return result->substr(2);
}

/// The `where` stage for a single predicate, which compares no numbers.
auto where(std::string_view predicate, bool raw = false)
  -> Option<std::string> {
  TRY(auto result, filtered({predicate}, raw));
  constexpr auto prefix = std::string_view{"| where "};
  REQUIRE(result.starts_with(prefix));
  return result.substr(prefix.size());
}

/// The stages that `plan_spl_aggregation` appends to the search `S`.
auto stages(ir::OptimizeAggregation const& aggregation, bool raw = false)
  -> Option<std::string> {
  auto result = aggregate("S", aggregation, raw);
  if (not result) {
    return None{};
  }
  REQUIRE(result->starts_with("S\n"));
  return result->substr(2);
}

/// Result rows, as Splunk's export endpoint returns them: one JSON object of
/// strings per row.
auto parse_rows(std::vector<std::string_view> objects)
  -> std::vector<nova::Array<nova::Record>> {
  auto dh = collecting_diagnostic_handler{};
  auto settings = nova::JsonParser::Settings{};
  settings.builder.raw = true;
  auto parser = nova::JsonParser::make(std::move(settings), dh);
  REQUIRE(parser);
  auto result = std::vector<nova::Array<nova::Record>>{};
  for (auto object : objects) {
    auto events = parser->parse_document(object);
    REQUIRE(events);
    result.push_back(std::move(events->data));
  }
  return result;
}

auto decode(std::vector<std::string_view> objects,
            ir::OptimizeAggregation const& aggregation, bool raw = false,
            bool checked = false)
  -> Result<std::vector<nova::Events>, std::string> {
  auto dh = collecting_diagnostic_handler{};
  auto rows = parse_rows(std::move(objects));
  auto result = decode_spl_aggregation(rows, aggregation, raw, checked, dh);
  CHECK(std::move(dh).collect().empty());
  return result;
}

/// The output events of a successful decoding, one string per event.
auto events(std::vector<std::string_view> objects,
            ir::OptimizeAggregation const& aggregation, bool raw = false,
            bool checked = false) -> std::vector<std::string> {
  auto decoded = decode(std::move(objects), aggregation, raw, checked);
  if (decoded.is_err()) {
    FAIL("unexpected error: {}", decoded.unwrap_err());
  }
  auto result = std::vector<std::string>{};
  for (auto const& batch : decoded.unwrap()) {
    for (auto row : nova::storage::true_bits(batch.mask)) {
      CHECK_EQUAL(*batch.meta.name.get(row), "tenzir.summarize");
      result.push_back(
        fmt::format("{}", nova::materialize_legacy(
                            nova::Array<nova::Data>{batch.data}.get(row))));
    }
  }
  return result;
}

/// The reason why decoding fails.
auto error(std::vector<std::string_view> objects,
           ir::OptimizeAggregation const& aggregation, bool raw = false,
           bool checked = false) -> std::string {
  auto decoded = decode(std::move(objects), aggregation, raw, checked);
  REQUIRE(decoded.is_err());
  return decoded.unwrap_err();
}

/// `summarize user, n=count(), a=count(action)`.
auto by_user() -> ir::OptimizeAggregation {
  return make({key({"user"}), call({"n"}, "count"),
               call({"a"}, "count", Path{"action"})});
}

} // namespace

TEST("aggregations become eval and stats stages") {
  CHECK_EQUAL(stages(make({call({"n"}, "count")})),
              "| stats count as tenzir_count\n"
              "| appendpipe [stats count as tenzir_groups]");
  CHECK_EQUAL(stages(by_user()),
              "| eval tenzir_aggregate_0=case(isnull('user'), \"n\", "
              "mvcount('user')!=1, \"m\", match('user', \"\\x00\"), \"z\", "
              "true(), \"v\" . 'user'), "
              "tenzir_aggregate_2=if(isnull('action'), null(), 1)\n"
              "| stats count as tenzir_count, count(tenzir_aggregate_2) as "
              "tenzir_aggregate_2 by tenzir_aggregate_0\n"
              "| appendpipe [stats count as tenzir_groups]");
  CHECK_EQUAL(stages(make({call({"lo"}, "min", Path{"n"}),
                           call({"hi"}, "max", Path{"n"})})),
              "| eval tenzir_aggregate_0=if(mvcount('n')==1 AND match('n', "
              "\"^-?[0-9]{1,15}$\"), tonumber('n'), null()), "
              "tenzir_check_0=if(isnull('n') OR (mvcount('n')==1 AND "
              "match('n', \"^-?[0-9]{1,15}$\")), 0, 1), "
              "tenzir_aggregate_1=if(mvcount('n')==1 AND match('n', "
              "\"^-?[0-9]{1,15}$\"), tonumber('n'), null()), "
              "tenzir_check_1=if(isnull('n') OR (mvcount('n')==1 AND "
              "match('n', \"^-?[0-9]{1,15}$\")), 0, 1)\n"
              "| stats count as tenzir_count, "
              "min(tenzir_aggregate_0) as tenzir_aggregate_0, "
              "max(tenzir_check_0) as tenzir_check_0, "
              "max(tenzir_aggregate_1) as tenzir_aggregate_1, "
              "max(tenzir_check_1) as tenzir_check_1\n"
              "| appendpipe [stats count as tenzir_groups]");
  // Field names that are not identifiers go into single quotes. Group keys
  // alone still count the results of each group.
  CHECK_EQUAL(stages(make({key({"k{}"}), key({"a b.c"})})),
              "| eval tenzir_aggregate_0=case(isnull('k{}'), \"n\", "
              "mvcount('k{}')!=1, \"m\", match('k{}', \"\\x00\"), \"z\", "
              "true(), \"v\" . 'k{}'), "
              "tenzir_aggregate_1=case(isnull('a b.c'), \"n\", "
              "mvcount('a b.c')!=1, \"m\", match('a b.c', \"\\x00\"), \"z\", "
              "true(), \"v\" . 'a b.c')\n"
              "| stats count as tenzir_count by tenzir_aggregate_0, "
              "tenzir_aggregate_1\n"
              "| appendpipe [stats count as tenzir_groups]");
  // Every `count()` reads the same column, since `stats` would name a repeated
  // function after its last alias only.
  CHECK_EQUAL(stages(make({call({"a"}, "count"), call({"b"}, "count")})),
              "| stats count as tenzir_count\n"
              "| appendpipe [stats count as tenzir_groups]");
}

TEST("aggregations that Splunk computes differently stay local") {
  // Functions other than `count`, `min`, and `max`.
  CHECK(not stages(make({call({"s"}, "sum", Path{"n"})})));
  CHECK(not stages(make({call({"d"}, "count_distinct", Path{"n"})})));
  // With `raw`, `min` and `max` see strings, which they reject.
  CHECK(not stages(make({call({"lo"}, "min", Path{"n"})}), true));
  CHECK(stages(by_user(), true));
  // Nested paths, which Splunk results do not have.
  CHECK(not stages(make({key({"a", "b"})})));
  // Internal fields, such as `_time`, and fields that the stages write.
  CHECK(not stages(make({key({"_time"})})));
  CHECK(not stages(make({call({"n"}, "count", Path{"_raw"})})));
  CHECK(not stages(make({key({"tenzir_aggregate_1"})})));
  // Names that `eval` reads differently.
  CHECK(not stages(make({key({"a|b"})})));
  CHECK(not stages(make({key({"a[b]"})})));
  CHECK(not stages(make({key({"a'b"})})));
  CHECK(not stages(make({key({"a\\b"})})));
  CHECK(not stages(make({key({"tag::x"})})));
  CHECK(not stages(make({key({"a`b"})})));
  CHECK(not stages(make({key({"a\"b"})})));
  CHECK(not stages(make({key({" a"})})));
  CHECK(not stages(make({key({""})})));
  // Searches that do not take more stages.
  CHECK(not aggregate("search \"unterminated", make({call({"n"}, "count")}),
                      false));
  CHECK(not aggregate("search x |", make({call({"n"}, "count")}), false));
  // Searches with side effects, which a second search would repeat.
  CHECK(not aggregate("search x | outputlookup users.csv",
                      make({call({"n"}, "count")}), false));
  CHECK(not aggregate("search x | collect index=summary",
                      make({call({"n"}, "count")}), false));
  CHECK(not aggregate("search `macro`", make({call({"n"}, "count")}), false));
  // Searches whose results show formatted values, which `stats` does not see.
  CHECK(not aggregate("search x | fieldformat n=n*2",
                      make({call({"n"}, "count")}), false));
  CHECK(not aggregate("search x | FieldFormat n=n*2",
                      make({call({"n"}, "count")}), false));
  // Searches whose results may lack fields, which Splunk does not return, but
  // `stats` counts.
  for (auto search : {
         "| makeresults | fields - _time",
         "| makeresults format=csv data=\"x\n1\" | fields y",
         "search x | fields -_raw",
         "search x | table user",
         "search x | rename _time as __t",
         "search x | stats count by user",
         "| makeresults | stats values(x) as v",
         "| inputlookup users.csv",
         "| tstats count where index=main",
         "search x | append [| makeresults | fields - _time]",
         "search [| inputlookup users.csv | fields user] | eval y=1",
         "search x | custom_command",
       }) {
    CHECK(not aggregate(search, make({call({"n"}, "count")}), false));
  }
  // Searches that start with events or `makeresults` and keep their fields.
  // Splunk returns results whose fields `eval` set to `null()` as empty
  // objects, which `summarize` counts as well.
  for (auto search : {
         "search index=main",
         "index=main sourcetype=json",
         "search index=main | eval x=1 | rex field=_raw \"(?<y>\\d+)\"",
         "search index=main | where x > 1 | fields user, action",
         "search index=main | fields + user | lookup users user OUTPUT name",
         "| makeresults count=3 | streamstats count as i | eval k=i",
         "| makeresults | eval _time=null()",
         "| search index=main | dedup user | sort - _time | head 10",
         "search [search index=other | fields user] | eval x=1",
       }) {
    CHECK(aggregate(search, make({call({"n"}, "count")}), false));
  }
}

TEST("sources decide on aggregations without a registry") {
  // `can_plan_spl_aggregation` checks everything but the filter, whose
  // translation needs a registry.
  CHECK(can_plan_spl_aggregation("search index=main", by_user(), false));
  CHECK(can_plan_spl_aggregation("search index=main", by_user(), true));
  CHECK(not can_plan_spl_aggregation("search x | outputlookup users.csv",
                                     by_user(), false));
  CHECK(not can_plan_spl_aggregation("search x | fieldformat n=n*2", by_user(),
                                     false));
  CHECK(
    not can_plan_spl_aggregation("| inputlookup users.csv", by_user(), false));
  CHECK(not can_plan_spl_aggregation(
    "search index=main", make({call({"lo"}, "min", Path{"n"})}), true));
  CHECK(not can_plan_spl_aggregation("search index=main",
                                     make({key({"_time"})}), false));
  CHECK(not can_plan_spl_aggregation(
    "search index=main", make({call({"s"}, "sum", Path{"n"})}), false));
}

TEST("aggregation results become the output of the summary") {
  auto result = events(
    {
      R"({"tenzir_aggregate_0":"valice","tenzir_count":"3",)"
      R"("tenzir_aggregate_2":"2"})",
      R"({"tenzir_aggregate_0":"v192.0.2.1","tenzir_count":"1",)"
      R"("tenzir_aggregate_2":"0"})",
      R"({"tenzir_aggregate_0":"v007","tenzir_count":"1",)"
      R"("tenzir_aggregate_2":"1"})",
      R"({"tenzir_aggregate_0":"v","tenzir_count":"1",)"
      R"("tenzir_aggregate_2":"1"})",
      R"({"tenzir_aggregate_0":"n","tenzir_count":"2",)"
      R"("tenzir_aggregate_2":"0"})",
      R"({"tenzir_groups":"5"})",
    },
    by_user());
  CHECK_EQUAL(result, (std::vector<std::string>{
                        R"(<user: "alice", n: 3, a: 2>)",
                        R"(<user: 192.0.2.1, n: 1, a: 0>)",
                        R"(<user: 7, n: 1, a: 1>)",
                        R"(<user: "", n: 1, a: 1>)",
                        R"(<user: null, n: 2, a: 0>)",
                      }));
  // With `raw`, keys stay strings, as in the results of a search.
  CHECK_EQUAL(events(
                {
                  R"({"tenzir_aggregate_0":"v007","tenzir_count":"1",)"
                  R"("tenzir_aggregate_2":"1"})",
                  R"({"tenzir_aggregate_0":"v7","tenzir_count":"1",)"
                  R"("tenzir_aggregate_2":"1"})",
                  R"({"tenzir_groups":"2"})",
                },
                by_user(), true),
              (std::vector<std::string>{
                R"(<user: "007", n: 1, a: 1>)",
                R"(<user: "7", n: 1, a: 1>)",
              }));
  // Output fields may be nested, and the cells of a row come in any order.
  auto nested = make({key({"user"}, Path{"by", "user"}),
                      call({"by", "n"}, "count"), call({"total"}, "count")});
  CHECK_EQUAL(events(
                {
                  R"({"tenzir_count":"4","tenzir_aggregate_0":"vbob"})",
                  R"({"tenzir_groups":"1"})",
                },
                nested),
              (std::vector<std::string>{
                R"(<by: <user: "bob", n: 4>, total: 4>)",
              }));
  // Without groups, the input of the summary was empty.
  CHECK(events({R"({"tenzir_groups":"0"})"}, by_user()).empty());
}

TEST("aggregations without group keys produce a single event") {
  auto counts = make({call({"n"}, "count"), call({"a"}, "count", Path{"x"}),
                      call({"lo"}, "min", Path{"x"})});
  // Without results, `min` and `max` have neither a value nor a check.
  CHECK_EQUAL(events(
                {
                  R"({"tenzir_count":"0","tenzir_aggregate_1":"0"})",
                  R"({"tenzir_groups":"1"})",
                },
                counts),
              (std::vector<std::string>{"<n: 0, a: 0, lo: null>"}));
  CHECK_EQUAL(events(
                {
                  R"({"tenzir_count":"2","tenzir_aggregate_1":"1",)"
                  R"("tenzir_aggregate_2":"-5","tenzir_check_2":"0"})",
                  R"({"tenzir_groups":"1"})",
                },
                counts),
              (std::vector<std::string>{"<n: 2, a: 1, lo: -5>"}));
  CHECK(decode(
          {
            R"({"tenzir_count":"2","tenzir_aggregate_1":"1",)"
            R"("tenzir_aggregate_2":"-5"})",
            R"({"tenzir_groups":"1"})",
          },
          counts)
          .is_err());
  CHECK_EQUAL(error({R"({"tenzir_groups":"0"})"}, counts),
              "expected a single row");
}

TEST("aggregations compute the minimum and maximum of integers") {
  auto extrema = make({key({"g"}), call({"lo"}, "min", Path{"n"}),
                       call({"hi"}, "max", Path{"n"})});
  CHECK_EQUAL(
    events(
      {
        R"({"tenzir_aggregate_0":"va","tenzir_aggregate_1":"-0",)"
        R"("tenzir_check_1":"0","tenzir_aggregate_2":"123456789012345",)"
        R"("tenzir_check_2":"0","tenzir_count":"4"})",
        // A group without values of the input.
        R"({"tenzir_aggregate_0":"vb","tenzir_check_1":"0",)"
        R"("tenzir_check_2":"0","tenzir_count":"2"})",
        R"({"tenzir_groups":"2"})",
      },
      extrema),
    (std::vector<std::string>{
      R"(<g: "a", lo: 0, hi: 123456789012345>)",
      R"(<g: "b", lo: null, hi: null>)",
    }));
  // A group with values other than integers aggregates locally.
  CHECK_EQUAL(error(
                {
                  R"({"tenzir_aggregate_0":"va","tenzir_aggregate_1":"5",)"
                  R"("tenzir_check_1":"1","tenzir_aggregate_2":"10",)"
                  R"("tenzir_check_2":"1","tenzir_count":"3"})",
                  R"({"tenzir_groups":"1"})",
                },
                extrema),
              "a group has a value for `min` or `max` other than an integer");
  CHECK_EQUAL(error(
                {
                  R"({"tenzir_aggregate_0":"va","tenzir_aggregate_1":"5.0",)"
                  R"("tenzir_check_1":"0","tenzir_aggregate_2":"10",)"
                  R"("tenzir_check_2":"0","tenzir_count":"3"})",
                  R"({"tenzir_groups":"1"})",
                },
                extrema),
              "a result of `min` or `max` is invalid");
  CHECK_EQUAL(error(
                {
                  R"({"tenzir_aggregate_0":"va","tenzir_aggregate_1":"5",)"
                  R"("tenzir_aggregate_2":"10","tenzir_check_2":"0",)"
                  R"("tenzir_count":"3"})",
                  R"({"tenzir_groups":"1"})",
                },
                extrema),
              "a group has a value for `min` or `max` other than an integer");
}

TEST("aggregation results that might differ from TQL are rejected") {
  constexpr auto alice = R"({"tenzir_aggregate_0":"valice","tenzir_count":"1",)"
                         R"("tenzir_aggregate_2":"1"})";
  // A search that fails in `eval` has an empty response.
  CHECK_EQUAL(error({}, by_user()), "the result misses the count of groups");
  CHECK_EQUAL(error({alice}, by_user()),
              "the result misses the count of groups");
  CHECK_EQUAL(error({alice, R"({"tenzir_groups":"2"})"}, by_user()),
              "received 1 of 2 groups");
  CHECK_EQUAL(error({R"({"tenzir_groups":"0"})", alice}, by_user()),
              "a row follows the count of groups");
  CHECK_EQUAL(error({R"({"tenzir_groups":"0","tenzir_aggregate_0":"vx"})"},
                    by_user()),
              "the count of groups has other fields");
  // `stats` would cut a key off at a NUL character.
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":"z","tenzir_count":"2",)"
                     R"("tenzir_aggregate_2":"1"})",
                     R"({"tenzir_groups":"1"})"},
                    by_user()),
              "a group key contains a NUL character");
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":"z","tenzir_count":"2",)"
                     R"("tenzir_aggregate_2":"1"})",
                     R"({"tenzir_groups":"1"})"},
                    by_user(), true),
              "a group key contains a NUL character");
  // A key with several values, which `summarize` groups as a list.
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":"m","tenzir_count":"1",)"
                     R"("tenzir_aggregate_2":"1"})",
                     R"({"tenzir_groups":"1"})"},
                    by_user()),
              "a group key has several values");
  // Type inference merges groups that Splunk keeps apart.
  auto merged = [&](std::string_view lhs, std::string_view rhs) {
    auto first
      = fmt::format(R"({{"tenzir_aggregate_0":"{}","tenzir_count":"1",)"
                    R"("tenzir_aggregate_2":"1"}})",
                    lhs);
    auto second
      = fmt::format(R"({{"tenzir_aggregate_0":"{}","tenzir_count":"1",)"
                    R"("tenzir_aggregate_2":"1"}})",
                    rhs);
    return decode({first, second, R"({"tenzir_groups":"2"})"}, by_user())
      .is_err();
  };
  CHECK(merged("v5", "v005"));
  CHECK(merged("v5", "v5.0"));
  CHECK(merged("v0.0", "v-0.0"));
  CHECK(merged("vnan", "vNaN"));
  CHECK(merged("v::1", "v0:0:0:0:0:0:0:1"));
  CHECK(merged("v1s", "v1000ms"));
  CHECK(not merged("valice", "vAlice"));
  CHECK(not merged("v5", "v 5"));
  CHECK(not merged("vnull", "n"));
  // Every result infers a relative time anew, so `summarize` groups each one
  // apart.
  auto relative = [&](std::string_view key, bool raw = false) {
    auto row = fmt::format(R"({{"tenzir_aggregate_0":"{}","tenzir_count":"2",)"
                           R"("tenzir_aggregate_2":"1"}})",
                           key);
    return decode({row, R"({"tenzir_groups":"1"})"}, by_user(), raw).is_err();
  };
  CHECK(relative("vnow"));
  CHECK(relative("vnow - 1h"));
  CHECK(relative("vin 5 min"));
  CHECK(relative("v5 min ago"));
  CHECK(not relative("vnow", true));
  CHECK(not relative("vinfo"));
  CHECK(not relative("vnowhere"));
  CHECK(not relative("v2025-01-01"));
  // Malformed cells.
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":"x","tenzir_count":"1",)"
                     R"("tenzir_aggregate_2":"1"})",
                     R"({"tenzir_groups":"1"})"},
                    by_user()),
              "a group key is invalid");
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":"vx","tenzir_count":"1.0",)"
                     R"("tenzir_aggregate_2":"1"})",
                     R"({"tenzir_groups":"1"})"},
                    by_user()),
              "a count is not a non-negative integer");
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":"vx","tenzir_count":"1"})",
                     R"({"tenzir_groups":"1"})"},
                    by_user()),
              "a count is not a non-negative integer");
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":"vx","tenzir_count":"0",)"
                     R"("tenzir_aggregate_2":"0"})",
                     R"({"tenzir_groups":"1"})"},
                    by_user()),
              "a group contains no results");
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":["va","vb"],)"
                     R"("tenzir_count":"1","tenzir_aggregate_2":"1"})",
                     R"({"tenzir_groups":"1"})"},
                    by_user()),
              "a field is not a single string");
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":"vx","tenzir_count":"1",)"
                     R"("tenzir_aggregate_2":"1","x":"1"})",
                     R"({"tenzir_groups":"1"})"},
                    by_user()),
              "unexpected field `x`");
}

TEST("exact filters go into a where stage before the aggregation") {
  // Strings that type inference keeps as strings compare byte by byte, and a
  // field with several values equals no literal.
  CHECK_EQUAL(where(R"(user == "alice")"),
              R"((mvcount('user')==1 AND 'user'=="alice"))");
  CHECK_EQUAL(where(R"("alice" == user)"),
              R"((mvcount('user')==1 AND 'user'=="alice"))");
  CHECK_EQUAL(where(R"(user == "")"), R"((mvcount('user')==1 AND 'user'==""))");
  CHECK_EQUAL(where(R"(user == "a\"b\\c*")"),
              R"((mvcount('user')==1 AND 'user'=="a\"b\\c*"))");
  CHECK_EQUAL(where(R"(user in ["alice", "bob"])"),
              R"((mvcount('user')==1 AND ('user'=="alice" OR 'user'=="bob")))");
  // Booleans are the values `true` and `false`.
  CHECK_EQUAL(where("flag"), R"((mvcount('flag')==1 AND 'flag'=="true"))");
  CHECK_EQUAL(where("flag == false"),
              R"((mvcount('flag')==1 AND 'flag'=="false"))");
  // Null checks.
  CHECK_EQUAL(where("action == null"), "isnull('action')");
  CHECK_EQUAL(where("action != null"), "isnotnull('action')");
  // Negations keep missing fields and fields with several values.
  CHECK_EQUAL(where(R"(user != "alice")"),
              R"((isnull('user') OR mvcount('user')>1 OR )"
              R"(if('user'=="alice", false(), true())))");
  CHECK_EQUAL(where(R"(not (user == "alice"))"),
              R"((isnull('user') OR mvcount('user')>1 OR )"
              R"(if('user'=="alice", false(), true())))");
  CHECK_EQUAL(where(R"(not (user in ["alice", "bob"]))"),
              R"((isnull('user') OR mvcount('user')>1 OR )"
              R"(if(('user'=="alice" OR 'user'=="bob"), false(), true())))");
  CHECK_EQUAL(where("flag != true"),
              R"((isnull('flag') OR mvcount('flag')>1 OR )"
              R"(if('flag'=="true", false(), true())))");
  // Junctions, and several filters, which all apply.
  CHECK_EQUAL(where(R"(user == "alice" or action == null)"),
              R"(((mvcount('user')==1 AND 'user'=="alice") OR )"
              R"(isnull('action')))");
  CHECK_EQUAL(filtered({R"(sourcetype == "linux_secure")",
                        R"(action != "logout" and user != null)"}),
              R"(| where (mvcount('sourcetype')==1 AND )"
              R"('sourcetype'=="linux_secure") AND ((isnull('action') OR )"
              R"(mvcount('action')>1 OR if('action'=="logout", false(), )"
              R"(true())) AND isnotnull('user')))");
  // Names that are not identifiers go into single quotes.
  CHECK_EQUAL(where(R"(this["a b.c"] == "x")"),
              R"((mvcount('a b.c')==1 AND 'a b.c'=="x"))");
  // With `raw`, every string stays a string.
  CHECK_EQUAL(where(R"(user == "5")", true),
              R"((mvcount('user')==1 AND 'user'=="5"))");
  CHECK_EQUAL(where(R"(user != "true")", true),
              R"((isnull('user') OR mvcount('user')>1 OR )"
              R"(if('user'=="true", false(), true())))");
}

TEST("exact filters check their comparisons of numbers") {
  constexpr auto integer = std::string_view{R"(match('n', "^-?[0-9]{1,15}$"))"};
  auto const uncertain = fmt::format(
    R"((NOT {} AND match('n', "[0-9]|[iI][nN][fF]|[nN][aA][nN]")))", integer);
  // The `where` stage keeps the results that pass with any outcome of the
  // uncertain comparisons, and the check flags those that do not pass without
  // them.
  auto stages = [](std::string_view upper, std::string_view lower) {
    return fmt::format("| where {}\n| eval tenzir_filter_check=if({}, 0, 1)\n"
                       "| stats count as tenzir_count, "
                       "max(tenzir_filter_check) "
                       "as tenzir_filter_check\n"
                       "| appendpipe [stats count as tenzir_groups]",
                       upper, lower);
  };
  auto positive = [&](std::string_view test) {
    return stages(fmt::format("(mvcount('n')==1 AND (({} AND {}) OR {}))",
                              integer, test, uncertain),
                  fmt::format("(mvcount('n')==1 AND {} AND {})", integer,
                              test));
  };
  CHECK_EQUAL(filtered({"n > 6"}), positive("tonumber('n')>6"));
  CHECK_EQUAL(filtered({"6 < n"}), positive("tonumber('n')>6"));
  CHECK_EQUAL(filtered({"n <= -1.5"}), positive("tonumber('n')<=-1.5"));
  CHECK_EQUAL(filtered({"n > 0.0001"}), positive("tonumber('n')>0.0001"));
  CHECK_EQUAL(filtered({"n == -999999999999999"}),
              positive("tonumber('n')==-999999999999999"));
  CHECK_EQUAL(filtered({"n in [5, 7]"}),
              positive("(tonumber('n')==5 OR tonumber('n')==7)"));
  auto negative = [&](std::string_view test) {
    return stages(
      fmt::format("(isnull('n') OR mvcount('n')>1 OR if({} AND {}, false(), "
                  "true()))",
                  integer, test),
      fmt::format("(isnull('n') OR mvcount('n')>1 OR if(({} AND {}) OR {}, "
                  "false(), true()))",
                  integer, test, uncertain));
  };
  CHECK_EQUAL(filtered({"n != 5"}), negative("tonumber('n')==5"));
  CHECK_EQUAL(filtered({"not (n in [5, 7])"}),
              negative("(tonumber('n')==5 OR tonumber('n')==7)"));
  // Other predicates are the same in both bounds.
  CHECK_EQUAL(filtered({R"(user == "alice" or n > 6)"}),
              stages(fmt::format("((mvcount('user')==1 AND 'user'==\"alice\") "
                                 "OR (mvcount('n')==1 AND (({} AND "
                                 "tonumber('n')>6) OR {})))",
                                 integer, uncertain),
                     fmt::format("((mvcount('user')==1 AND 'user'==\"alice\") "
                                 "OR (mvcount('n')==1 AND {} AND "
                                 "tonumber('n')>6))",
                                 integer)));
}

TEST("filters without an exact spelling keep the aggregation local") {
  for (auto predicate : {
         // Strings that type inference turns into other types.
         R"(user == "5")",
         R"(user == "true")",
         R"(user == "192.0.2.1")",
         R"(user == "now")",
         R"(user != "5")",
         R"(user in ["alice", "5"])",
         // Control characters.
         "user == \"a\\nb\"",
         // Ordering on strings and booleans.
         R"(user < "b")",
         "flag > false",
         // Numbers that `search` would read with an exponent, or with more
         // than 15 significant digits.
         "n > 0.0000001",
         "n > 1.2345678901234567",
         "n == 1000000000000000",
         "n == 9007199254740993",
         // Lists of several types, whose later elements TQL never matches.
         R"(n in [1, "a"])",
         R"(not (n in [1, "a"]))",
         // Lists in which TQL's `in` finds booleans and numbers alike.
         "n in [0, 5]",
         "not (n in [1, 5])",
         "flag in [true]",
         // Negations of other predicates.
         "not (n > 5)",
         "not flag",
         R"(not (user.starts_with("a")))",
         // Other predicates.
         R"(user.starts_with("a"))",
         R"(user.match_regex("^a"))",
         R"(user.to_lower() == "alice")",
         R"("lic" in user)",
         "src == 192.0.2.1",
         "src.ip() in 10.0.0.0/8",
         "_time > 2025-01-01",
         "t > 2025-01-01",
         "n + 1 > 5",
         "n > m",
         // Fields that `eval` reads differently, nested fields, and fields
         // that the stages write.
         R"(_raw == "x")",
         R"(this["a|b"] == "x")",
         R"(a.b == "x")",
         R"(tenzir_x == "x")",
       }) {
    CHECK(not filtered({predicate}));
    // A single predicate without a spelling keeps all of them local.
    CHECK(not filtered({R"(user == "alice")", predicate}));
  }
  // Without inference, a string differs from every number and boolean.
  CHECK(not filtered({"n > 5"}, true));
  CHECK(not filtered({"n != 5"}, true));
  CHECK(not filtered({"flag"}, true));
  CHECK(not filtered({"flag == true"}, true));
}

TEST("the fallback keeps every result that may pass the filters") {
  auto fallback = [](std::vector<std::string_view> predicates,
                     Option<std::vector<std::string_view>> fields = None{},
                     bool raw = false) -> Option<std::string> {
    auto filter = ir::OptimizeFilter{};
    for (auto predicate : predicates) {
      filter.push_back(parse(predicate));
    }
    auto projection = Option<ir::OptimizeProjection>{};
    if (fields) {
      projection.emplace();
      for (auto field : *fields) {
        auto path = ast::field_path::try_from(parse(field));
        REQUIRE(path);
        projection->push_back(std::move(*path));
      }
    }
    TRY(auto result, plan_spl_aggregation("S", filter, projection,
                                          make({call({"n"}, "count")}), raw,
                                          test::builtin_registry()));
    return std::move(result.fallback);
  };
  // Without filters, the search only keeps the fields that the summary reads.
  CHECK_EQUAL(fallback({}, std::vector<std::string_view>{"user"}),
              "S\n| fields user");
  CHECK_EQUAL(fallback({}), "S\n| fields *");
  // The `where` stage of the summary replaces the prefilters. It keeps the
  // values whose comparison with a number is uncertain, such as `1e999`,
  // which type inference reads as infinity, but for which Splunk's `search`
  // command finds `n>6` false. The fields that the filters read remain.
  constexpr auto integer = std::string_view{R"(match('n', "^-?[0-9]{1,15}$"))"};
  CHECK_EQUAL(fallback({"n > 6"}, std::vector<std::string_view>{"user"}),
              fmt::format("S\n| where (mvcount('n')==1 AND (({0} AND "
                          "tonumber('n')>6) OR "
                          "(NOT {0} AND match('n', "
                          "\"[0-9]|[iI][nN][fF]|[nN][aA][nN]\"))))\n"
                          "| fields user, n",
                          integer));
  CHECK_EQUAL(fallback({R"(user == "alice")", "action != null"}),
              "S\n| where (mvcount('user')==1 AND 'user'==\"alice\") AND "
              "isnotnull('action')\n| fields *");
  CHECK_EQUAL(fallback({R"(user != "5")"}, None{}, true),
              "S\n| where (isnull('user') OR mvcount('user')>1 OR "
              "if('user'==\"5\", false(), true()))\n| fields *");
  // Filters without an exact spelling never reach a pushed summary.
  CHECK(not fallback({R"(user.starts_with("a"))"}));
  // Both searches share the `where` stage, after which the search checks the
  // comparisons of numbers.
  auto planned = plan_spl_aggregation("S", {parse("n > 6")}, None{},
                                      make({call({"n"}, "count")}), false,
                                      test::builtin_registry());
  REQUIRE(planned);
  CHECK(planned->checked);
  auto where
    = planned->fallback.substr(0, planned->fallback.find("\n| fields"));
  CHECK(planned->search.starts_with(
    fmt::format("{}\n| eval tenzir_filter_check=", where)));
}

TEST("a filter check decides whether the aggregation holds") {
  auto const aggregation = make({call({"n"}, "count")});
  constexpr auto raw = false;
  constexpr auto checked = true;
  CHECK_EQUAL(events({R"({"tenzir_count":"3","tenzir_filter_check":"0"})",
                      R"({"tenzir_groups":"1"})"},
                     aggregation, raw, checked),
              std::vector<std::string>{"<n: 3>"});
  // Without results, `stats` computes no check.
  CHECK_EQUAL(events({R"({"tenzir_count":"0"})", R"({"tenzir_groups":"1"})"},
                     aggregation, raw, checked),
              std::vector<std::string>{"<n: 0>"});
  CHECK_EQUAL(error({R"({"tenzir_count":"3","tenzir_filter_check":"1"})",
                     R"({"tenzir_groups":"1"})"},
                    aggregation, raw, checked),
              "a filter compares a number with a value other than an integer");
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":"valice","tenzir_count":"1",)"
                     R"("tenzir_aggregate_2":"1","tenzir_filter_check":"0"})",
                     R"({"tenzir_aggregate_0":"vbob","tenzir_count":"1",)"
                     R"("tenzir_aggregate_2":"1","tenzir_filter_check":"1"})",
                     R"({"tenzir_groups":"2"})"},
                    by_user(), raw, checked),
              "a filter compares a number with a value other than an integer");
  // A search that computes the check must deliver it for every group with
  // results.
  CHECK_EQUAL(error({R"({"tenzir_count":"3"})", R"({"tenzir_groups":"1"})"},
                    aggregation, raw, checked),
              "a group misses the check of the filter");
  CHECK_EQUAL(error({R"({"tenzir_aggregate_0":"valice","tenzir_count":"1",)"
                     R"("tenzir_aggregate_2":"1","tenzir_filter_check":"0"})",
                     R"({"tenzir_aggregate_0":"vbob","tenzir_count":"1",)"
                     R"("tenzir_aggregate_2":"1"})",
                     R"({"tenzir_groups":"2"})"},
                    by_user(), raw, checked),
              "a group misses the check of the filter");
  // A search without the check has no such field.
  CHECK_EQUAL(error({R"({"tenzir_count":"3","tenzir_filter_check":"0"})",
                     R"({"tenzir_groups":"1"})"},
                    aggregation),
              "unexpected field `tenzir_filter_check`");
}
