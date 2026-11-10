//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The stages that `from_clickhouse` takes over: what it spells from their
// syntax, how it binds their fields to the columns of a table, and the
// queries that result. The integration tests in
// `test/tests/operators/from_clickhouse/` run such queries against a server.

#include "clickhouse/query_plan.hpp"

#include "clickhouse/sql_pushdown.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/pushdown/expr_render.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/registry.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <fmt/format.h>

#include <limits>
#include <string_view>

namespace tenzir::plugins::clickhouse {

namespace {

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

/// Spells `source` as a predicate with the syntax-only dialect.
auto spell(std::string_view source) -> std::string {
  auto dialect = ClickHouseDialect{};
  auto renderer = pushdown::ExpressionRenderer{
    dialect, [&](std::span<std::string const> path) {
      return dialect.bind_field(path);
    }};
  return renderer.predicate(parse(source)).value_or("<none>");
}

auto column(std::string name, std::string type, std::string default_kind = "",
            std::string comment = "") -> ColumnDescription {
  return {std::move(name), std::move(type), std::move(default_kind), "",
          std::move(comment)};
}

auto plain_table() -> std::vector<ColumnDescription> {
  return {
    column("id", "UInt64"),
    column("x", "Int64"),
    column("Y", "String"),
    column("meta", "Tuple(source String, inner Tuple(level Nullable(Int64)))"),
    column("a.b", "Int64"),
    column("j", "JSON"),
    column("typed", "JSON(k String)"),
    column("j.shadow", "String"),
    column("m", "Map(String, Int64)"),
    column("p", "Point"),
    column("half", "Tuple(ok String, bad Point)"),
    column("al", "String", "ALIAS"),
    column("mat", "Int64", "MATERIALIZED"),
  };
}

auto catch_all_table() -> std::vector<ColumnDescription> {
  return {
    column("time", "DateTime64(9)"),
    column("src_endpoint.port", "UInt16"),
    column("t", "Tuple(a String)"),
    column("unmapped", "JSON"),
    column("event.a", "String"),
    column("g", "String", "ALIAS"),
    column("event", "JSON", "", "tenzir:catch_all"),
  };
}

auto make_binding(std::vector<ColumnDescription> description) -> TableBinding {
  auto dh = collecting_diagnostic_handler{};
  auto catch_all = make_catch_all_layout(description, dh);
  REQUIRE(catch_all);
  return TableBinding{std::move(description), std::move(*catch_all)};
}

/// Binds `path` and renders the expression, or the reason why it fails.
auto bind(TableBinding const& table, std::vector<std::string> path)
  -> std::string {
  auto result = table.bind(path);
  if (result.is_err()) {
    return fmt::format("error: {}", result.unwrap_err());
  }
  auto const& field = result.unwrap();
  return fmt::format("{}{}", field.sql, field.dynamic ? " (dynamic)" : "");
}

auto filter(std::string_view predicate) -> ir::QueryStage {
  return {ir::QueryStage::Filter{parse(predicate)}};
}

auto select(
  std::vector<std::pair<std::vector<std::string>, std::vector<std::string>>>
    fields) -> ir::QueryStage {
  auto result = ir::QueryStage::Select{};
  for (auto& [output, input] : fields) {
    result.fields.push_back({.output = output, .input = input});
  }
  return {std::move(result)};
}

auto drop(std::vector<std::vector<std::string>> fields) -> ir::QueryStage {
  return {ir::QueryStage::Drop{std::move(fields)}};
}

auto aggregate(std::vector<ir::AggregateColumn> columns) -> ir::QueryStage {
  return {ir::QueryStage::Aggregate{.columns = std::move(columns),
                                    .schema_name = "tenzir.summarize"}};
}

auto key(std::vector<std::string> field) -> ir::AggregateColumn {
  return {.output = field, .function = None{}, .input = field};
}

auto call(std::string output, std::string function,
          Option<std::vector<std::string>> input = None{})
  -> ir::AggregateColumn {
  return {.output = {std::move(output)},
          .function = std::move(function),
          .input = std::move(input)};
}

auto sort(std::vector<std::string> field, bool descending = false)
  -> ir::QueryStage {
  return {ir::QueryStage::Sort{
    .keys = {{.field = std::move(field), .descending = descending}},
    .bound = None{}}};
}

auto limit(uint64_t count) -> ir::QueryStage {
  return {ir::QueryStage::Limit{count}};
}

/// Binds `plan` and describes the error, if any.
auto bind_error(TableBinding const& table, ir::QueryPlan const& plan)
  -> std::string {
  auto result = bind_plan(table, plan);
  if (result.is_ok()) {
    return "ok";
  }
  auto const& error = result.unwrap_err();
  auto const kind = error.kind == BindError::Kind::field   ? "field"
                    : error.kind == BindError::Kind::stage ? "stage"
                                                           : "sort";
  return fmt::format("{} `{}`: {}", kind, error.subject, error.reason);
}

/// Renders the query that reads `plan` from `tbl`.
auto query(TableBinding const& table, ir::QueryPlan const& plan,
           std::string_view selection = "*") -> std::string {
  auto bound = bind_plan(table, plan);
  REQUIRE(bound.is_ok());
  auto columns = std::vector<std::string>{"id", "x"};
  auto read = make_table_read("tbl", selection, plan, columns, bound.unwrap());
  if (read.is_err()) {
    return fmt::format("error: {}", read.unwrap_err());
  }
  return read.unwrap().sql;
}

} // namespace

TEST("literals spell in ClickHouse SQL") {
  CHECK_EQUAL(spell("x == 1"), "(`x` = 1)");
  // Negative literals are negations, which ClickHouse folds as TQL does.
  CHECK_EQUAL(spell("x == -9223372036854775808"),
              "(`x` = (-9223372036854775808))");
  CHECK_EQUAL(spell("x == 18446744073709551615"),
              "(`x` = 18446744073709551615)");
  CHECK_EQUAL(spell("x == 2.0"), "(`x` = 2.)");
  CHECK_EQUAL(spell("x == -0.5"), "(`x` = (-0.5))");
  CHECK_EQUAL(spell("x == true"), "(`x` = TRUE)");
  CHECK_EQUAL(spell(R"(x == "it's \\ a")"), R"((`x` = 'it\'s \\ a'))");
  CHECK_EQUAL(spell(R"(x == "a\nb")"), R"((`x` = 'a\x0Ab'))");
  CHECK_EQUAL(spell("x == 10.0.0.1"), "(`x` = toIPv4('10.0.0.1'))");
  CHECK_EQUAL(spell("x == ::1"), "(`x` = toIPv6('::1'))");
  // Times are nanoseconds since the epoch, also before 1900 and before the
  // epoch with a fraction.
  CHECK_EQUAL(spell("x > 2024-01-01T00:00:00.5"),
              "(`x` > fromUnixTimestamp64Nano(1704067200500000000, 'UTC'))");
  CHECK_EQUAL(spell("x < 1969-12-31T23:59:59.5"),
              "(`x` < fromUnixTimestamp64Nano(-500000000, 'UTC'))");
  CHECK_EQUAL(ClickHouseDialect{}
                .render_time(time{} + duration{-2524348800000000000})
                .value_or("<none>"),
              "fromUnixTimestamp64Nano(-2524348800000000000, 'UTC')");
  // Durations, subnets, and non-finite numbers have no spelling.
  CHECK_EQUAL(spell("x > 1h"), "<none>");
  CHECK_EQUAL(spell("x in 10.0.0.0/8"), "<none>");
}

TEST("operators and functions spell in ClickHouse SQL") {
  CHECK_EQUAL(spell("x == null"), "(`x` IS NULL)");
  CHECK_EQUAL(spell("not (x != null)"), "(NOT (`x` IS NOT NULL))");
  CHECK_EQUAL(spell("x in [1, 2]"), "(`x` IN (1, 2))");
  CHECK_EQUAL(spell("x + 1 > y * 2 or flag"),
              "(((`x` + 1) > (`y` * 2)) OR `flag`)");
  CHECK_EQUAL(spell("-x > 0"), "((-`x`) > 0)");
  CHECK_EQUAL(spell(R"(x.starts_with("a") and x.ends_with("b"))"),
              "(startsWith(`x`, 'a') AND endsWith(`x`, 'b'))");
  CHECK_EQUAL(spell("x.length_bytes() > x.length_chars()"),
              "(length(`x`) > lengthUTF8(`x`))");
  CHECK_EQUAL(spell(R"(x.to_lower() == x.to_upper().trim())"),
              "(lowerUTF8(`x`) = trimBoth(upperUTF8(`x`)))");
  CHECK_EQUAL(spell("x.abs() > 1"), "(abs(`x`) > 1)");
  CHECK_EQUAL(spell(R"(x.match_regex("^a."))"), "match(`x`, '^a.')");
  CHECK_EQUAL(spell(R"(x.starts_with("a", ignore_case=true))"), "<none>");
  CHECK_EQUAL(spell(R"(x.capitalize() == "A")"), "<none>");
}

TEST("fields spell from their syntax before binding") {
  CHECK_EQUAL(spell("meta.inner.level > 1"),
              "(tupleElement(tupleElement(`meta`, 'inner'), 'level') > 1)");
  CHECK_EQUAL(spell(R"(this["a.b"] == 1)"), "(`a.b` = 1)");
  CHECK_EQUAL(spell(R"(this["a`b\\c"] == 1)"), R"((`a\`b\\c` = 1))");
  CHECK_EQUAL(spell(R"(this[""] == 1)"), "<none>");
  CHECK_EQUAL(spell("x? == 1"), "<none>");
}

TEST("aggregations and exclusions spell in ClickHouse SQL") {
  auto const dialect = ClickHouseDialect{};
  CHECK_EQUAL(dialect.render_aggregate("count", None{}).value_or("<none>"),
              "toInt64(count())");
  CHECK_EQUAL(
    dialect.render_aggregate("count", std::string{"`x`"}).value_or("<none>"),
    "toInt64(count(`x`))");
  CHECK_EQUAL(
    dialect.render_aggregate("min", std::string{"`x`"}).value_or("<none>"),
    "minOrNull(`x`)");
  CHECK_EQUAL(
    dialect.render_aggregate("max", std::string{"`x`"}).value_or("<none>"),
    "maxOrNull(`x`)");
  // A sum or a mean may yield `NaN`, which the operator reads as `null`.
  CHECK(not dialect.render_aggregate("sum", std::string{"`x`"}));
  CHECK(not dialect.render_aggregate("mean", std::string{"`x`"}));
  CHECK(not dialect.render_aggregate("min", None{}));
  auto columns = std::vector<std::string>{"a", "b`c"};
  CHECK_EQUAL(dialect.render_exclude(columns).value_or("<none>"),
              R"(* EXCEPT (`a`, `b\`c`))");
  CHECK_EQUAL(dialect.max_limit(), std::numeric_limits<uint64_t>::max());
}

TEST("absorption takes over the stages that ClickHouse spells") {
  auto chain = ir::QueryChain{
    .stages = {filter("x > 0"), sort({"x"}, true), limit(3),
               filter("x.capitalize() == \"A\"")},
    .end = "",
  };
  auto absorption = absorb_stages(chain);
  CHECK_EQUAL(absorption.prefix, size_t{3});
  CHECK_EQUAL(absorption.declined, "ClickHouse cannot spell the stage");
  chain.stages = {aggregate({key({"y"}), call("n", "count")}), filter("n > 1")};
  CHECK_EQUAL(absorb_stages(chain).prefix, size_t{2});
  chain.stages = {aggregate({call("s", "sum", std::vector<std::string>{"x"})})};
  CHECK_EQUAL(absorb_stages(chain).prefix, size_t{0});
  // Names that start with `__tenzir_` belong to the query.
  chain.stages = {filter("__tenzir_x > 0")};
  CHECK_EQUAL(absorb_stages(chain).prefix, size_t{0});
}

TEST("fields of a plain table bind to its columns") {
  auto const table = make_binding(plain_table());
  CHECK_EQUAL(bind(table, {"x"}), "`x`");
  CHECK_EQUAL(bind(table, {"meta", "source"}),
              "tupleElement(`meta`, 'source')");
  CHECK_EQUAL(bind(table, {"meta", "inner", "level"}),
              "tupleElement(tupleElement(`meta`, 'inner'), 'level')");
  // A literal dotted name is the dotted column, while a path is not.
  CHECK_EQUAL(bind(table, {"a.b"}), "`a.b`");
  CHECK_EQUAL(bind(table, {"a", "b"}), "tupleElement(`a`, 'b')");
  // ClickHouse resolves names with regard to case, and rejects unknown ones.
  CHECK_EQUAL(bind(table, {"y"}), "`y`");
  CHECK_EQUAL(bind(table, {"Y"}), "`Y`");
  CHECK_EQUAL(bind(table, {"nope"}), "`nope`");
  // Values below a `JSON` column are dynamic, the whole column is not.
  CHECK_EQUAL(bind(table, {"j"}), "`j`");
  CHECK_EQUAL(bind(table, {"j", "k.l", "m"}), "`j`.`k%2El`.`m` (dynamic)");
  CHECK_EQUAL(bind(table, {"m"}), "`m`");
  CHECK_EQUAL(bind(table, {"j", "shadow"}),
              "error: the column `j.shadow` shadows the field below the `JSON` "
              "column `j`");
  CHECK_EQUAL(bind(table, {"typed", "k"}),
              "error: the `JSON` column `typed` has typed or skipped paths");
  CHECK_EQUAL(bind(table, {"meta", "nope"}),
              "error: the tuple `meta` has no element `nope`");
  CHECK_EQUAL(bind(table, {"x", "y"}),
              "error: `x` has the type `Int64`, which has no fields");
  CHECK_EQUAL(bind(table, {"m", "k"}),
              "error: `m` has the type `Map(String,Int64)`, which has no "
              "fields");
  CHECK_EQUAL(bind(table, {"p"}), "error: the column `p` has the type `Point`, "
                                  "which the operator cannot read");
  // An element of a tuple reads on its own, also next to an element that
  // does not decode, while the whole tuple and that element do not.
  CHECK_EQUAL(bind(table, {"half", "ok"}), "tupleElement(`half`, 'ok')");
  CHECK_EQUAL(bind(table, {"half", "bad"}),
              "error: `half.bad` has the type `Point`, which the operator "
              "cannot read");
  CHECK_EQUAL(bind(table, {"half"}),
              "error: the column `half` has the type `Tuple(ok String, bad "
              "Point)`, which the operator cannot read");
  CHECK_EQUAL(bind_error(table, {select({{{"value"}, {"half", "ok"}}})}), "ok");
  CHECK_EQUAL(bind(table, {"al"}),
              "error: `al` is an `ALIAS` column, which `SELECT *` includes "
              "only depending on the session's settings");
  CHECK_EQUAL(bind(table, {"mat"}),
              "error: `mat` is an `MATERIALIZED` column, which `SELECT *` "
              "includes only depending on the session's settings");
}

TEST("fields of a catch-all table bind to their single origin") {
  auto const table = make_binding(catch_all_table());
  CHECK_EQUAL(bind(table, {"time"}), "`time`");
  CHECK_EQUAL(bind(table, {"src_endpoint", "port"}), "`src_endpoint.port`");
  CHECK_EQUAL(bind(table, {"t", "a"}), "tupleElement(`t`, 'a')");
  CHECK_EQUAL(bind(table, {"src_endpoint", "hostname"}),
              "`event`.`src_endpoint`.`hostname` (dynamic)");
  CHECK_EQUAL(bind(table, {"x.y"}), "`event`.`x%2Ey` (dynamic)");
  CHECK_EQUAL(bind(table, {"unmapped", "k"}), "`unmapped`.`k` (dynamic)");
  CHECK_EQUAL(bind(table, {"src_endpoint"}),
              "error: the field has no single column in the table: several "
              "columns hold parts of it, or a generated column may hold it");
  CHECK_EQUAL(bind(table, {"g"}),
              "error: the field has no single column in the table: several "
              "columns hold parts of it, or a generated column may hold it");
  CHECK_EQUAL(bind(table, {"unmapped"}),
              "error: the field is the whole `JSON` column `unmapped`, from "
              "which the operator reconstructs a record");
  CHECK_EQUAL(bind(table, {"a", "b"}),
              "error: the column `event.a` shadows the field below the `JSON` "
              "column `event`");
}

TEST("null checks of values below a JSON column consider objects") {
  auto const plain = make_binding(plain_table());
  auto const present = std::string{
    "((`j`.`a` IS NOT NULL) OR notEmpty(JSONAllPaths(`j`.^`a`)))"};
  CHECK_EQUAL(query(plain, {filter("j.a != null")}),
              fmt::format("SELECT * FROM tbl WHERE {}", present));
  CHECK_EQUAL(query(plain, {filter("j.a == null")}),
              fmt::format("SELECT * FROM tbl WHERE (NOT {})", present));
  CHECK_EQUAL(query(plain, {filter("null != j.a.b")}),
              "SELECT * FROM tbl WHERE ((`j`.`a`.`b` IS NOT NULL) OR "
              "notEmpty(JSONAllPaths(`j`.^`a`.`b`)))");
  // Comparisons with literals and null checks of columns stay as they are.
  CHECK_EQUAL(query(plain, {filter("j.a == 5")}),
              "SELECT * FROM tbl WHERE (`j`.`a` = 5)");
  CHECK_EQUAL(query(plain, {filter("x == null")}),
              "SELECT * FROM tbl WHERE (`x` IS NULL)");
  // A projection may name its result like the field below the `JSON` column.
  CHECK_EQUAL(query(plain,
                    {filter("j.a != null"), select({{{"j", "a"}, {"x"}}}),
                     limit(1), filter("j.a != null")}),
              fmt::format("SELECT * FROM (SELECT `x` AS `__tenzir_column_0` "
                          "FROM tbl WHERE {} LIMIT 1) AS `__tenzir_level_1` "
                          "WHERE (`__tenzir_column_0` IS NOT NULL)",
                          present));
  auto const catch_all = make_binding(catch_all_table());
  CHECK_EQUAL(query(catch_all, {filter("src_endpoint.hostname == null")}),
              "SELECT * FROM tbl WHERE (NOT "
              "((`event`.`src_endpoint`.`hostname` "
              "IS NOT NULL) OR "
              "notEmpty(JSONAllPaths(`event`.^`src_endpoint`.`hostname`))))");
}

TEST("sorts by values below a JSON column end the order") {
  auto const plain = make_binding(plain_table());
  auto const error
    = std::string{"sort `j.k`: the query would return the order of the sort "
                  "for the stages after it in a column of a dynamic type, "
                  "which the operator cannot read"};
  CHECK_EQUAL(bind_error(plain, {sort({"j", "k"}), limit(1)}), "ok");
  CHECK_EQUAL(bind_error(plain, {sort({"j", "k"}), filter("x > 0")}), "ok");
  CHECK_EQUAL(bind_error(plain, {sort({"j", "k"}), limit(1),
                                 aggregate({call("n", "count")})}),
              "ok");
  CHECK_EQUAL(bind_error(plain, {sort({"j", "k"}), limit(1), sort({"id"})}),
              error);
  CHECK_EQUAL(bind_error(plain, {sort({"j", "k"}), limit(1), filter("x > 0")}),
              error);
  CHECK_EQUAL(bind_error(plain, {sort({"j", "k"}), select({{{"i"}, {"id"}}}),
                                 filter("i > 0")}),
              error);
  CHECK_EQUAL(bind_error(plain, {sort({"x"}), limit(1), sort({"id"})}), "ok");
  auto const catch_all = make_binding(catch_all_table());
  CHECK_EQUAL(bind_error(catch_all, {sort({"src_endpoint", "hostname"}),
                                     limit(1), sort({"time"})}),
              "sort `src_endpoint.hostname`: the query would return the order "
              "of the sort for the stages after it in a column of a dynamic "
              "type, which the operator cannot read");
}

TEST("narrowed tuples read their elements unambiguously") {
  auto schema = SqlSchema{};
  schema.add_column("a", "Tuple(b Int64, c Tuple(d String, e Int64))");
  schema.add_column("a.b", "Int64");
  auto projection = ir::OptimizeProjection{};
  projection.push_back(*ast::field_path::try_from(parse("a.c.d")));
  projection.push_back(*ast::field_path::try_from(parse("a.b")));
  // `a`.`b` would read the column `a.b`.
  CHECK_EQUAL(select_columns(&schema, projection, TupleAccess::element),
              "CAST(tuple(tupleElement(`a`, 'b'), "
              "CAST(tuple(tupleElement(tupleElement(`a`, 'c'), 'd')), "
              "'Tuple(`d` String)')), 'Tuple(`b` Int64, `c` Tuple(`d` "
              "String))') AS `a`");
  // Hydrolix keeps the dotted spelling.
  CHECK_EQUAL(select_columns(&schema, projection, TupleAccess::dotted),
              "CAST(tuple(`a`.`b`, CAST(tuple(`a`.`c`.`d`), 'Tuple(`d` "
              "String)')), 'Tuple(`b` Int64, `c` Tuple(`d` String))') AS `a`");
}

TEST("plans bind the fields that they read from the table") {
  auto const plain = make_binding(plain_table());
  CHECK_EQUAL(bind_error(plain, {filter(R"(j.k == "v")"), sort({"j", "n"})}),
              "ok");
  auto bound = bind_plan(plain, ir::QueryPlan{filter(R"(j.k == "v")"),
                                              sort({"j", "n"})});
  REQUIRE(bound.is_ok());
  CHECK(bound.unwrap().reads_dynamic);
  CHECK(bound.unwrap().sorts_dynamic);
  CHECK(not bound.unwrap().summarizes);
  CHECK_EQUAL(bind_error(plain, {select({{{"v"}, {"j", "k"}}})}),
              "field `j.k`: the field lies below a `JSON` column and has a "
              "dynamic type, which the operator cannot read as a result "
              "column");
  CHECK_EQUAL(
    bind_error(plain, {aggregate({call("n", "count",
                                       std::vector<std::string>{"j", "k"})})}),
    "field `j.k`: the field lies below a `JSON` column and has a "
    "dynamic type, which the operator cannot read as a result "
    "column");
  CHECK_EQUAL(bind_error(plain, {filter("p == null")}),
              "field `p`: the column `p` has the type `Point`, which the "
              "operator cannot read");
  // The stages after a selection read its result.
  bound
    = bind_plan(plain, ir::QueryPlan{select({{{"p"}, {"x"}}}), filter("p > 0"),
                                     aggregate({call("n", "count")})});
  REQUIRE(bound.is_ok());
  CHECK(bound.unwrap().summarizes);
  CHECK_EQUAL(bound.unwrap().fields.size(), size_t{1});
  auto const catch_all = make_binding(catch_all_table());
  CHECK_EQUAL(bind_error(catch_all, {filter("src_endpoint.port == 443"),
                                     sort({"time"}), limit(1)}),
              "ok");
  CHECK_EQUAL(bind_error(catch_all, {select({{{"time"}, {"time"}}})}),
              "field `time`: `select` on a table with the catch-all column "
              "`event` reads fields that may live in any of its columns");
  CHECK_EQUAL(bind_error(catch_all, {drop({{"time"}})}),
              "field `time`: `drop` on a table with the catch-all column "
              "`event` reads fields that may live in any of its columns");
  CHECK_EQUAL(bind_error(catch_all, {aggregate({call("n", "count")})}),
              "stage `summarize`: `summarize` on a table with the catch-all "
              "column `event` reads fields that may live in any of its "
              "columns");
}

TEST("table reads compute the stages in their query") {
  auto const table = make_binding(plain_table());
  CHECK_EQUAL(query(table, {}), "SELECT * FROM tbl");
  CHECK_EQUAL(query(table, {filter("meta.source == \"a\""), limit(0)},
                    "`id`, `meta`"),
              "SELECT * FROM (SELECT `id`, `meta` FROM tbl) AS "
              "`__tenzir_table` WHERE (tupleElement(`meta`, 'source') = 'a') "
              "LIMIT 0");
  CHECK_EQUAL(query(table, {drop({{"x"}, {"p"}}), limit(2)}),
              "SELECT * EXCEPT (`x`, `p`) FROM tbl LIMIT 2");
  CHECK_EQUAL(query(table, {limit(5), filter("x > 0")}),
              "SELECT * FROM (SELECT * FROM tbl LIMIT 5) AS `__tenzir_level_0` "
              "WHERE (`x` > 0)");
  CHECK_EQUAL(query(table, {limit(5), aggregate({call("n", "count")})}),
              "SELECT toInt64(count()) AS `__tenzir_column_1` FROM (SELECT * "
              "FROM tbl LIMIT 5) AS `__tenzir_level_0`");
  CHECK_EQUAL(
    query(table, {filter("x > 0"), aggregate({key({"Y"}), call("n", "count")}),
                  filter("n >= 2"), sort({"n"}, true), limit(3)}),
    "SELECT * FROM (SELECT `Y` AS `__tenzir_column_0`, toInt64(count()) AS "
    "`__tenzir_column_1` FROM tbl WHERE (`x` > 0) GROUP BY `Y`) AS "
    "`__tenzir_level_2` WHERE (`__tenzir_column_1` >= 2) ORDER BY "
    "`__tenzir_column_1` DESC LIMIT 3");
  CHECK_EQUAL(query(table, {sort({"x"}, true), limit(2), sort({"id"})}),
              "SELECT * FROM (SELECT `__tenzir_source_1`.*, `x` AS "
              "`__tenzir_order_0` FROM tbl AS `__tenzir_source_1` ORDER BY "
              "`x` DESC LIMIT 2) AS `__tenzir_level_2` ORDER BY `id` ASC, "
              "`__tenzir_order_0` DESC");
}

} // namespace tenzir::plugins::clickhouse
