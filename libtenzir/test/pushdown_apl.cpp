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

#include <limits>
#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::pushdown;
using namespace std::literals;

namespace {

using Path = std::vector<std::string>;

auto field_at(std::vector<std::string> const& path) -> ast::expression {
  auto result = ast::expression{
    ast::root_field{ast::identifier{path.front(), location::unknown}, false}};
  for (auto i = size_t{1}; i < path.size(); ++i) {
    result = ast::field_access{std::move(result), location::unknown, false,
                               ast::identifier{path[i], location::unknown}};
  }
  return result;
}

auto parse(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
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

auto expr_key(Path output, std::string_view expr) -> ir::AggregateColumn {
  return {
    .output = std::move(output), .function = None{}, .args = {parse(expr)}};
}

auto call(Path output, std::string function,
          std::vector<ast::expression> args = {}) -> ir::AggregateColumn {
  return {.output = std::move(output),
          .function = std::move(function),
          .args = std::move(args)};
}

auto aggregate(std::vector<ir::AggregateColumn> columns) -> ir::QueryStage {
  return {ir::QueryStage::Aggregate{.columns = std::move(columns),
                                    .schema_name = "tenzir.summarize"}};
}

auto sort(std::vector<std::string_view> keys) -> ir::QueryStage {
  auto result = ir::QueryStage::Sort{};
  for (auto k : keys) {
    auto desc = k.starts_with('-');
    if (desc) {
      k.remove_prefix(1);
    }
    result.keys.push_back({.field = {std::string{k}}, .descending = desc});
  }
  return {std::move(result)};
}

auto limit(uint64_t count) -> ir::QueryStage {
  return {ir::QueryStage::Limit{count}};
}

auto apl(std::vector<ir::QueryStage> stages, std::string_view base = "['test']")
  -> std::string {
  auto planned
    = apl_planner("Axiom", std::string{base}, None{}).plan(stages, nullptr);
  if (planned.is_err()) {
    return "-";
  }
  auto value = std::move(planned).unwrap();
  CHECK_EQUAL(value.query, std::string{base} + value.generated);
  return value.generated;
}

auto where(std::string_view predicate) -> std::string {
  auto text = apl({filter(predicate)});
  if (text == "-") {
    return {};
  }
  constexpr auto prefix = "\n| where "sv;
  REQUIRE(text.starts_with(prefix));
  return text.substr(prefix.size());
}

} // namespace

TEST("apl function deltas") {
  CHECK_EQUAL(where(R"(s.trim() == "hi")"),
              R"((trim_regex(@"\s+", ['s']) == "hi"))");
  CHECK_EQUAL(where(R"(s.trim_start() == "hi")"),
              R"((trim_start_regex(@"\s+", ['s']) == "hi"))");
  CHECK_EQUAL(where(R"(s.trim_end() == "hi")"),
              R"((trim_end_regex(@"\s+", ['s']) == "hi"))");
  // APL `replace_string` takes (lookup, rewrite, text).
  CHECK_EQUAL(where(R"(s.replace("a", "b") == "b")"),
              R"((replace_string("a", "b", ['s']) == "b"))");
  // APL `floor` takes 1 argument.
  CHECK_EQUAL(where("x.floor() == 2"), "(tolong(floor(['x'])) == 2)");
}

TEST("apl nanosecond literals and epoch-aligned bin") {
  CHECK_EQUAL(where("_time == 2026-01-02T03:04:05.123456789Z"),
              "(['_time'] == datetime(2026-01-02T03:04:05.123456789Z))");
  CHECK_EQUAL(where("d == 10ns"), "(['d'] == timespan(10ns))");
  CHECK_EQUAL(where("d == -1s"), "(['d'] == (-timespan(1000000000ns)))");
  // Folded negative duration constant renders directly as `timespan(-Nns)`.
  auto neg_dur = ast::binary_expr{
    field_at({"d"}), ast::binary_op::eq,
    ast::constant::make(located{data{duration{-1s}}, location::unknown})};
  CHECK_EQUAL(apl({ir::QueryStage{ir::QueryStage::Filter{neg_dur}}}),
              "\n| where (['d'] == timespan(-1000000000ns))");
  // `int64_min` nanoseconds fails to parse in APL.
  auto min_dur = ast::binary_expr{
    field_at({"d"}), ast::binary_op::eq,
    ast::constant::make(located{data{duration::min()}, location::unknown})};
  CHECK_EQUAL(apl({ir::QueryStage{ir::QueryStage::Filter{min_dur}}}), "-");
  // Non-2-day divisor (3d) and nanosecond width both work in APL `bin`.
  CHECK_EQUAL(where("_time.floor(3d) == 2026-01-01T00:00:00Z"),
              "(bin(['_time'], timespan(259200000000000ns)) == "
              "datetime(2026-01-01T00:00:00.000000000Z))");
  CHECK_EQUAL(where("_time.floor(10ns) == 2026-01-02T03:04:05.123456780Z"),
              "(bin(['_time'], timespan(10ns)) == "
              "datetime(2026-01-02T03:04:05.123456780Z))");
}

TEST("apl rejects take 0") {
  CHECK_EQUAL(apl({limit(0)}), "-");
  CHECK_EQUAL(apl({limit(10)}), "\n| take 10");
}

TEST("apl bessel-rescaled population stddev and variance") {
  CHECK_EQUAL(apl({aggregate({
                call({"sd"}, "stddev", {field_at({"v"})}),
                call({"vr"}, "variance", {field_at({"v"})}),
              })}),
              "\n| summarize "
              "['__tenzir_column_0'] = (stdev(['v']) * "
              "sqrt(todouble(countif(isnotnull(['v'])) - 1) / "
              "countif(isnotnull(['v'])))), "
              "['__tenzir_column_1'] = (variance(['v']) * "
              "(todouble(countif(isnotnull(['v'])) - 1) / "
              "countif(isnotnull(['v']))))");
}

TEST("apl exact two-stage count_distinct") {
  // Standalone grouped count_distinct needs no dummy stage-1 aggregation.
  CHECK_EQUAL(apl({aggregate({
                key({"g"}),
                call({"d"}, "count_distinct", {field_at({"x"})}),
              })}),
              "\n| summarize by ['g'], ['x']"
              "\n| summarize ['__tenzir_column_0'] = countif(isnotnull(['x'])) "
              "by ['g']");
  // Standalone ungrouped count_distinct.
  CHECK_EQUAL(apl({aggregate({
                call({"d"}, "count_distinct", {field_at({"x"})}),
              })}),
              "\n| summarize by ['x']"
              "\n| summarize ['__tenzir_column_0'] = "
              "countif(isnotnull(['x']))");
  // With decomposable siblings (count, sum, min, max, all, any).
  CHECK_EQUAL(apl({aggregate({
                key({"g"}),
                call({"d"}, "count_distinct", {field_at({"x"})}),
                call({"c"}, "count"),
                call({"s"}, "sum", {field_at({"v"})}),
                call({"lo"}, "min", {field_at({"v"})}),
                call({"a"}, "all", {field_at({"b"})}),
              })}),
              "\n| summarize "
              "['__tenzir_part_0'] = count(), "
              "['__tenzir_part_1'] = sum(['v']), "
              "['__tenzir_part_2'] = min(['v']), "
              "['__tenzir_part_3'] = countif(['b'] == false) "
              "by ['g'], ['x']"
              "\n| summarize "
              "['__tenzir_column_0'] = countif(isnotnull(['x'])), "
              "['__tenzir_column_1'] = sum(['__tenzir_part_0']), "
              "['__tenzir_column_2'] = sum(['__tenzir_part_1']), "
              "['__tenzir_column_3'] = min(['__tenzir_part_2']), "
              "['__tenzir_column_4'] = (sum(['__tenzir_part_3']) == 0) "
              "by ['g']");
  // Distinct on a group key avoids duplicating the key in stage 1.
  CHECK_EQUAL(apl({aggregate({
                key({"g"}),
                call({"d"}, "count_distinct", {field_at({"g"})}),
              })}),
              "\n| summarize by ['g']"
              "\n| summarize ['__tenzir_column_0'] = countif(isnotnull(['g'])) "
              "by ['g']");
  // Declines two distinct fields or non-decomposable siblings (never dcount).
  CHECK_EQUAL(apl({aggregate({
                call({"a"}, "count_distinct", {field_at({"x"})}),
                call({"b"}, "count_distinct", {field_at({"y"})}),
              })}),
              "-");
  CHECK_EQUAL(apl({aggregate({
                call({"d"}, "count_distinct", {field_at({"x"})}),
                call({"m"}, "mean", {field_at({"v"})}),
              })}),
              "-");
  CHECK_EQUAL(apl({aggregate({
                call({"d"}, "count_distinct", {field_at({"x"})}),
                call({"s"}, "stddev", {field_at({"v"})}),
              })}),
              "-");
}

TEST("apl dotted field quotes as one segment") {
  CHECK_EQUAL(where(R"(this["geo.city"] == "Paris")"),
              R"((['geo.city'] == "Paris"))");
  CHECK_EQUAL(apl_dataset_query("my-logs"), Option<std::string>{"['my-logs']"});
  CHECK_EQUAL(apl_dataset_query("a'b"), Option<std::string>{R"(['a\'b'])"});
  CHECK(not apl_dataset_query(""));
}

TEST("apl query mode handles block comments safely") {
  // Block comments with a statement separator are declined; simple invalid
  // block comments stay untouched or fail in Axiom as invalid APL.
  CHECK(not kql_accepts_trailing_stages("['t'] /* ; */ | where x == 1"));
}

TEST("apl end-to-end demo plan") {
  auto const base = apl_dataset_query("test");
  REQUIRE(base);
  CHECK_EQUAL(
    apl(
      {
        filter("status >= 400 and not (src_ip in 10.0.0.0/8)"),
        aggregate({
          expr_key({"bucket"}, "_time.floor(5min)"),
          key({"method"}),
          call({"errors"}, "count"),
          call({"unique_ips"}, "count_distinct", {field_at({"src_ip"})}),
        }),
        sort({"-errors"}),
        limit(10),
      },
      *base),
    "\n| where ((['status'] >= 400) and not(ipv4_is_in_range(['src_ip'], "
    "\"10.0.0.0/8\")))"
    "\n| extend ['__tenzir_column_0'] = bin(['_time'], "
    "timespan(300000000000ns))"
    "\n| summarize ['__tenzir_part_0'] = count() by ['__tenzir_column_0'], "
    "['method'], ['src_ip']"
    "\n| summarize ['__tenzir_column_1'] = sum(['__tenzir_part_0']), "
    "['__tenzir_column_2'] = countif(isnotnull(['src_ip'])) by "
    "['__tenzir_column_0'], ['method']"
    "\n| sort by ['__tenzir_column_1'] desc"
    "\n| take 10");
}
