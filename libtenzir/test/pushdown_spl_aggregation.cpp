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
#include "tenzir/test/test.hpp"

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

/// The stages that `plan_spl_aggregation` appends to the search `S`.
auto stages(ir::OptimizeAggregation const& aggregation, bool raw = false)
  -> Option<std::string> {
  auto result = plan_spl_aggregation("S", aggregation, raw);
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
            ir::OptimizeAggregation const& aggregation, bool raw = false)
  -> Result<std::vector<nova::Events>, std::string> {
  auto dh = collecting_diagnostic_handler{};
  auto rows = parse_rows(std::move(objects));
  auto result = decode_spl_aggregation(rows, aggregation, raw, dh);
  CHECK(std::move(dh).collect().empty());
  return result;
}

/// The output events of a successful decoding, one string per event.
auto events(std::vector<std::string_view> objects,
            ir::OptimizeAggregation const& aggregation, bool raw = false)
  -> std::vector<std::string> {
  auto decoded = decode(std::move(objects), aggregation, raw);
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
           ir::OptimizeAggregation const& aggregation, bool raw = false)
  -> std::string {
  auto decoded = decode(std::move(objects), aggregation, raw);
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
  CHECK(not plan_spl_aggregation("search \"unterminated",
                                 make({call({"n"}, "count")}), false));
  CHECK(not plan_spl_aggregation("search x |", make({call({"n"}, "count")}),
                                 false));
  // Searches with side effects, which a second search would repeat.
  CHECK(not plan_spl_aggregation("search x | outputlookup users.csv",
                                 make({call({"n"}, "count")}), false));
  CHECK(not plan_spl_aggregation("search x | collect index=summary",
                                 make({call({"n"}, "count")}), false));
  CHECK(not plan_spl_aggregation("search `macro`", make({call({"n"}, "count")}),
                                 false));
  // Searches whose results show formatted values, which `stats` does not see.
  CHECK(not plan_spl_aggregation("search x | fieldformat n=n*2",
                                 make({call({"n"}, "count")}), false));
  CHECK(not plan_spl_aggregation("search x | FieldFormat n=n*2",
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
    CHECK(
      not plan_spl_aggregation(search, make({call({"n"}, "count")}), false));
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
    CHECK(plan_spl_aggregation(search, make({call({"n"}, "count")}), false));
  }
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
