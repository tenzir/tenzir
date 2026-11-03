//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/aggregation.hpp"
#include "tenzir/pushdown/renderer.hpp"
#include "tenzir/pushdown/sql.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

using namespace tenzir;
using namespace tenzir::pushdown;

namespace {

auto reg() -> registry const& {
  return test::builtin_registry();
}

auto parse(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, reg());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
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

auto quote(std::string_view name) -> std::string {
  return fmt::format("\"{}\"", name);
}

/// Spells the IR in a minimal SQL dialect.
class TestRenderer final : public SqlRenderer {
private:
  auto quote_identifier(std::string_view name) const -> std::string override {
    return quote(name);
  }

  auto quote_string(std::string_view text) const -> std::string override {
    return fmt::format("'{}'", text);
  }

  auto render_double(double x) const -> std::string override {
    return fmt::format("{}e0", x);
  }
};

/// Quotes identifiers with double quotes, and spells limits with `LIMIT`, or
/// with `TOP` up to 100.
class TestDialect final : public SqlQueryDialect {
public:
  explicit TestDialect(LimitClause clause = LimitClause::limit)
    : clause_{clause} {
  }

  auto quote_identifier(std::string_view name) const -> std::string override {
    return quote(name);
  }

  auto limit_clause() const -> LimitClause override {
    return clause_;
  }

  auto max_limit() const -> uint64_t override {
    return clause_ == LimitClause::top ? 100 : SqlQueryDialect::max_limit();
  }

private:
  LimitClause clause_;
};

/// Groups by integers and strings, and takes the extremes of integers.
class TestAggregateDialect final : public SqlAggregateDialect {
public:
  explicit TestAggregateDialect(std::vector<std::string> names)
    : names_{std::move(names)} {
  }

private:
  auto alias_collides(std::string_view alias) const -> bool override {
    return std::ranges::contains(names_, alias);
  }

  auto render_value(Expr const& expr) const -> Option<std::string> override {
    return renderer_.render(expr);
  }

  auto render_key(std::span<std::string const> path, FieldValue const& field,
                  std::string const& value) const
    -> Option<std::string> override {
    if (is<StringType>(field.column.type)) {
      return fmt::format("any({})", quote(path.front()));
    }
    if (is<IntType>(field.column.type)) {
      return value;
    }
    return None{};
  }

  auto render_count(Option<std::string> const& value) const
    -> std::string override {
    return fmt::format("count({})", value ? *value : std::string{"*"});
  }

  auto render_extremum(Extremum extremum, FieldValue const& field,
                       std::string const& value) const
    -> Option<std::string> override {
    if (not is<IntType>(field.column.type)) {
      return None{};
    }
    return fmt::format("{}({})", extremum == Extremum::min ? "min" : "max",
                       value);
  }

  std::vector<std::string> names_;
  TestRenderer renderer_;
};

auto make_columns() -> ColumnModel {
  auto result = ColumnModel{};
  result.add({"i"}, {.type = IntType{.bits = 64, .is_signed = true},
                     .nullable = true});
  result.add({"s"}, {.type = StringType{}, .nullable = true});
  result.add({"f"}, {.type = FloatType{}, .nullable = true});
  return result;
}

auto column(AggregationPlan::Kind kind, std::string alias)
  -> AggregationPlan::Column {
  return {.kind = std::move(kind), .alias = std::move(alias)};
}

auto aggregate(std::vector<AggregationPlan::Column> columns,
               std::vector<std::string> names = {"i", "s", "f"})
  -> Option<std::string> {
  auto where = std::vector<std::string>{"\"i\" > 0"};
  TRY(auto select, make_aggregate_select(AggregationPlan{std::move(columns)},
                                         make_columns(), TestDialect{},
                                         TestAggregateDialect{std::move(names)},
                                         "\"t\"", where));
  return render_select(select, TestDialect{});
}

} // namespace

TEST("selects render their clauses in order") {
  auto select = SqlSelect{
    .selection = "*",
    .from = "t",
    .where = {},
    .group_by = {},
    .order_by = {},
    .limit = None{},
  };
  CHECK_EQUAL(render_select(select, TestDialect{}), "SELECT * FROM t");
  select = SqlSelect{
    .selection = "a, count(*)",
    .from = "t",
    .where = {"x > 0", "y < 1"},
    .group_by = {"a", "b"},
    .order_by = {},
    .limit = 0,
  };
  CHECK_EQUAL(render_select(select, TestDialect{}),
              "SELECT a, count(*) FROM t WHERE x > 0 AND y < 1 GROUP BY a, b "
              "LIMIT 0");
  CHECK_EQUAL(render_select(select, TestDialect{LimitClause::top}),
              "SELECT TOP (0) a, count(*) FROM t WHERE x > 0 AND y < 1 GROUP "
              "BY a, b");
}

TEST("table plans push what translates and select what remains") {
  auto const columns = std::vector<SqlColumn>{
    {.name = "i", .visible = true},
    {.name = "s", .visible = true},
    {.name = "f", .visible = true},
    {.name = "hidden", .visible = false},
  };
  auto plan = [&](std::vector<std::string_view> filter,
                  Option<ir::OptimizeProjection> projection,
                  std::vector<std::string> keep = {}) {
    return make_table_plan(columns, make_columns(), TestRenderer{},
                           TestDialect{}, filters(std::move(filter)),
                           projection, reg(), keep);
  };
  // Everything translates, so the filter's columns need not be selected.
  auto x = plan({"i > 0", R"(s == "a")"}, paths({"s"}));
  CHECK_EQUAL(x.selection, "\"s\"");
  CHECK_EQUAL(x.pushed, (std::vector<std::string>{
                          "\"i\" > 0",
                          "(\"s\" IS NOT NULL AND \"s\" = 'a')",
                        }));
  CHECK(x.local.empty());
  // A predicate that stays local keeps its columns, and so does `keep`, in
  // table order. Hidden columns are never selected.
  x = plan({"i > 0", "f + i > 1"}, paths({"hidden", "s"}), {"i"});
  CHECK_EQUAL(x.selection, "\"i\", \"s\", \"f\"");
  CHECK_EQUAL(x.pushed, (std::vector<std::string>{"\"i\" > 0"}));
  CHECK_EQUAL(x.local.size(), size_t{1});
  // A projection without visible columns still needs the rows, and one
  // without a projection or with a local function call every field.
  x = plan({}, paths({"hidden", "missing"}));
  CHECK_EQUAL(x.selection, "\"i\"");
  x = plan({"i > 0"}, None{});
  CHECK_EQUAL(x.selection, "*");
  x = plan({R"(s.to_upper() == "A")"}, paths({"s"}));
  CHECK_EQUAL(x.selection, "*");
  // Without columns, everything stays local.
  auto unknown
    = make_table_plan({}, make_columns(), TestRenderer{}, TestDialect{},
                      filters({"i > 0"}), paths({"i"}), reg());
  CHECK_EQUAL(unknown.selection, "*");
  CHECK(unknown.pushed.empty());
  CHECK_EQUAL(unknown.local.size(), size_t{1});
}

TEST("table selects push a limit only without local predicates") {
  auto plan = TablePlan{
    .selection = "\"i\"",
    .pushed = {"\"i\" > 0"},
    .local = {},
  };
  auto query = [&](Option<uint64_t> limit, LimitClause clause) {
    auto const dialect = TestDialect{clause};
    return render_select(make_table_select(plan, "\"t\"", limit, dialect),
                         dialect);
  };
  CHECK_EQUAL(query(None{}, LimitClause::limit),
              "SELECT \"i\" FROM \"t\" WHERE \"i\" > 0");
  CHECK_EQUAL(query(0, LimitClause::limit),
              "SELECT \"i\" FROM \"t\" WHERE \"i\" > 0 LIMIT 0");
  CHECK_EQUAL(query(100, LimitClause::top),
              "SELECT TOP (100) \"i\" FROM \"t\" WHERE \"i\" > 0");
  // The dialect accepts no larger limit.
  CHECK_EQUAL(query(101, LimitClause::top),
              "SELECT \"i\" FROM \"t\" WHERE \"i\" > 0");
  plan.local = filters({"f + i > 1"});
  CHECK_EQUAL(query(10, LimitClause::limit),
              "SELECT \"i\" FROM \"t\" WHERE \"i\" > 0");
}

TEST("aggregations select each column under its alias") {
  using Path = std::vector<std::string>;
  CHECK_EQUAL(aggregate({
                column(AggregationPlan::Key{{"s"}}, "k0"),
                column(AggregationPlan::Key{{"i"}}, "k1"),
                column(AggregationPlan::Count{None{}}, "n"),
                column(AggregationPlan::Count{Path{"f"}}, "c"),
                column(AggregationPlan::Min{{"i"}}, "lo"),
                column(AggregationPlan::Max{{"i"}}, "hi"),
              }),
              std::string{
                "SELECT any(\"s\") AS \"k0\", \"i\" AS \"k1\", count(*) AS "
                "\"n\", count(\"f\") AS \"c\", min(\"i\") AS \"lo\", "
                "max(\"i\") AS \"hi\" FROM \"t\" WHERE \"i\" > 0 GROUP BY "
                "\"s\", \"i\""});
  // Without group keys, the query has no `GROUP BY`.
  CHECK_EQUAL(aggregate({column(AggregationPlan::Count{None{}}, "n")}),
              std::string{"SELECT count(*) AS \"n\" FROM \"t\" WHERE \"i\" > "
                          "0"});
}

TEST("aggregations stay local where the dialect or the columns veto") {
  using Path = std::vector<std::string>;
  auto count = column(AggregationPlan::Count{None{}}, "n");
  CHECK(not aggregate({}));
  CHECK(not aggregate({column(AggregationPlan::Key{{"f"}}, "k"), count}));
  CHECK(not aggregate({column(AggregationPlan::Min{{"s"}}, "lo")}));
  CHECK(not aggregate({column(AggregationPlan::Max{{"f"}}, "hi")}));
  CHECK(not aggregate({column(AggregationPlan::Key{{"missing"}}, "k"), count}));
  CHECK(not aggregate({column(AggregationPlan::Count{Path{"missing"}}, "c")}));
  CHECK(not aggregate({count}, {"i", "n"}));
}

namespace {

/// Spells plans in a minimal dialect with exclusions, one function, and times.
class PlanDialect final : public SqlQueryDialect {
public:
  explicit PlanDialect(LimitClause clause = LimitClause::limit)
    : clause_{clause} {
  }

  auto quote_identifier(std::string_view name) const -> std::string override {
    return quote(name);
  }

  auto limit_clause() const -> LimitClause override {
    return clause_;
  }

  auto bind_field(std::span<std::string const> path) const
    -> Option<std::string> override {
    auto result = quote(path.front());
    for (auto const& segment : path.subspan(1)) {
      result = fmt::format("{}.{}", result, quote(segment));
    }
    return result;
  }

  auto render_time(tenzir::time value) const -> Option<std::string> override {
    return fmt::format("TIMESTAMP '{}'", value);
  }

  auto
  render_call(std::string_view name, std::span<std::string const> args) const
    -> Option<std::string> override {
    if (name == "starts_with" and args.size() == 2) {
      return fmt::format("starts_with({}, {})", args[0], args[1]);
    }
    return None{};
  }

  auto render_exclude(std::span<std::string const> columns) const
    -> Option<std::string> override {
    auto quoted = std::vector<std::string>{};
    for (auto const& column : columns) {
      quoted.push_back(quote(column));
    }
    return fmt::format("* EXCLUDE ({})", fmt::join(quoted, ", "));
  }

private:
  LimitClause clause_;
};

using Path = std::vector<std::string>;

auto filter(std::string_view predicate) -> ir::QueryStage {
  return {ir::QueryStage::Filter{parse(predicate)}};
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

auto aggregate(std::vector<ir::AggregateColumn> columns) -> ir::QueryStage {
  return {ir::QueryStage::Aggregate{.columns = std::move(columns),
                                    .schema_name = "tenzir.summarize"}};
}

auto sort(std::vector<std::string_view> keys) -> ir::QueryStage {
  auto result = ir::QueryStage::Sort{.keys = {}, .bound = None{}};
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

/// Renders `plan` over the table `t`.
auto plan(std::vector<ir::QueryStage> stages,
          PlanDialect const& dialect = PlanDialect{}) -> Option<SqlPlan> {
  return render_plan(SqlRelation{.from = "\"t\"", .fields = None{}}, stages,
                     dialect);
}

auto sql(std::vector<ir::QueryStage> stages, PlanDialect const& dialect
                                             = PlanDialect{}) -> std::string {
  auto result = plan(std::move(stages), dialect);
  REQUIRE(result);
  return render_select(result->select, dialect);
}

auto layout(SqlLayout const& layout) -> std::string {
  auto fields = std::vector<std::string>{};
  if (layout.fields) {
    for (auto const& [field, column] : *layout.fields) {
      fields.push_back(fmt::format("{}={}", fmt::join(field, "."), column));
    }
  }
  return fmt::format("fields={} hidden=[{}] schema={}",
                     layout.fields
                       ? fmt::format("[{}]", fmt::join(fields, ", "))
                       : std::string{"*"},
                     fmt::join(layout.hidden, ", "),
                     layout.schema_name.value_or("-"));
}

} // namespace

TEST("plans join stages into one select where SQL's order allows it") {
  CHECK_EQUAL(sql({filter("x > 1 and y.z == \"a\""), filter("w != null"),
                   sort({"-x", "y"}), limit(5)}),
              std::string{"SELECT * FROM \"t\" WHERE ((\"x\" > 1) AND "
                          "(\"y\".\"z\" = 'a')) AND (\"w\" IS NOT NULL) ORDER "
                          "BY \"x\" DESC, \"y\" ASC LIMIT 5"});
  // A filter after a sort joins the same select, before the order.
  CHECK_EQUAL(sql({sort({"x"}), filter("x > 1"), limit(5)}),
              std::string{"SELECT * FROM \"t\" WHERE (\"x\" > 1) ORDER BY "
                          "\"x\" ASC LIMIT 5"});
  // A later sort breaks its ties by the earlier one, and limits combine.
  CHECK_EQUAL(sql({sort({"x"}), sort({"-y"}), limit(5), limit(3)}),
              std::string{"SELECT * FROM \"t\" ORDER BY \"y\" DESC, \"x\" ASC "
                          "LIMIT 3"});
  // Calls and literals that the dialect spells.
  CHECK_EQUAL(sql({filter("x.starts_with(\"a\") or t > 2024-01-01")}),
              std::string{
                "SELECT * FROM \"t\" WHERE (starts_with(\"x\", 'a') "
                "OR (\"t\" > TIMESTAMP '2024-01-01 00:00:00.000000000'))"});
}

TEST("plans select, rename, and drop fields") {
  auto selected = plan(
    {filter("a > 1"), select({{{"x"}, {"a"}}, {{"y", "z"}, {"b", "c"}}})});
  REQUIRE(selected);
  CHECK_EQUAL(render_select(selected->select, PlanDialect{}),
              std::string{
                "SELECT \"a\" AS \"__tenzir_column_0\", \"b\".\"c\" AS "
                "\"__tenzir_column_1\" FROM \"t\" WHERE (\"a\" > 1)"});
  CHECK_EQUAL(layout(selected->layout),
              std::string{"fields=[x=__tenzir_column_0, y.z=__tenzir_column_1] "
                          "hidden=[] schema=-"});
  // A filter after a selection reads its output fields.
  CHECK_EQUAL(sql({select({{{"x"}, {"a"}}}), filter("x > 1")}),
              std::string{
                "SELECT * FROM (SELECT \"a\" AS \"__tenzir_column_0\" FROM "
                "\"t\") AS \"__tenzir_level_1\" WHERE (\"__tenzir_column_0\" > "
                "1)"});
  CHECK(not plan({select({{{"x"}, {"a"}}}), filter("a > 1")}));
  // An empty selection keeps the rows.
  auto empty = plan({select({})});
  REQUIRE(empty);
  CHECK_EQUAL(render_select(empty->select, PlanDialect{}),
              std::string{"SELECT 1 AS \"__tenzir_row_0\" FROM \"t\""});
  CHECK_EQUAL(layout(empty->layout),
              std::string{"fields=[] hidden=[__tenzir_row_0] schema=-"});
  // Dropping from a table excludes columns, and dropping from a selection
  // keeps the other fields.
  CHECK_EQUAL(sql({filter("a > 1"), drop({{"b"}, {"c"}})}),
              std::string{"SELECT * EXCLUDE (\"b\", \"c\") FROM \"t\" WHERE "
                          "(\"a\" > 1)"});
  CHECK_EQUAL(sql({select({{{"x"}, {"a"}}, {{"y"}, {"b"}}}), drop({{"y"}})}),
              std::string{
                "SELECT \"__tenzir_column_0\" FROM (SELECT \"a\" AS "
                "\"__tenzir_column_0\", \"b\" AS \"__tenzir_column_1\" FROM "
                "\"t\") AS \"__tenzir_level_2\""});
  // Dropping an excluded column again does nothing, also with other columns
  // or behind a derived table.
  CHECK_EQUAL(sql({drop({{"b"}}), drop({{"b"}})}),
              std::string{"SELECT * EXCLUDE (\"b\") FROM \"t\""});
  CHECK_EQUAL(sql({drop({{"b"}}), filter("c > 1"), drop({{"b"}, {"c"}})}),
              std::string{"SELECT * EXCLUDE (\"c\") FROM (SELECT * EXCLUDE "
                          "(\"b\") FROM \"t\") AS \"__tenzir_level_0\" WHERE "
                          "(\"c\" > 1)"});
  // Later stages cannot read a column that a drop excluded, also behind a
  // derived table.
  CHECK(not plan({drop({{"b"}}), filter("b > 1")}));
  CHECK(not plan({drop({{"b"}}), limit(5), filter("b > 1")}));
  CHECK(not plan({drop({{"b"}}), sort({"b"})}));
  CHECK(not plan({drop({{"b"}}), select({{{"x"}, {"b"}}})}));
  CHECK_EQUAL(sql({drop({{"b"}}), limit(5), filter("c > 1")}),
              std::string{"SELECT * FROM (SELECT * EXCLUDE (\"b\") FROM \"t\" "
                          "LIMIT 5) AS \"__tenzir_level_0\" WHERE (\"c\" > "
                          "1)"});
  // Nested fields of a table, and parts of a selected record, do not drop.
  CHECK(not plan({drop({{"a", "b"}})}));
  CHECK(not plan({select({{{"x"}, {"a"}}}), drop({{"x", "y"}})}));
  // A record that loses its last field stays as an empty record, which no
  // layout expresses, so that drop stays in the pipeline.
  CHECK(not plan({select({{{"a", "b"}, {"x"}}}), drop({{"a", "b"}})}));
  CHECK(not plan({select({{{"a", "b"}, {"x"}}, {{"a", "c"}, {"y"}}}),
                  drop({{"a", "b"}, {"a", "c"}})}));
  auto partial = plan(
    {select({{{"a", "b"}, {"x"}}, {{"a", "c"}, {"y"}}}), drop({{"a", "b"}})});
  REQUIRE(partial);
  CHECK_EQUAL(layout(partial->layout),
              std::string{"fields=[a.c=__tenzir_column_1] hidden=[] schema=-"});
  // Dropping a whole record leaves no empty record behind.
  auto whole
    = plan({select({{{"a", "b"}, {"x"}}, {{"y"}, {"y"}}}), drop({{"a"}})});
  REQUIRE(whole);
  CHECK_EQUAL(layout(whole->layout),
              std::string{"fields=[y=__tenzir_column_1] hidden=[] schema=-"});
}

TEST("plans aggregate and filter, sort, and limit the groups") {
  auto summary = plan(
    {filter("v > 0"),
     aggregate({{.output = {"k"}, .function = None{}, .input = Path{"key"}},
                {.output = {"n"}, .function = "count", .input = None{}},
                {.output = {"m"}, .function = "max", .input = Path{"v"}}}),
     filter("n >= 10"), sort({"-n", "k"}), limit(20)});
  REQUIRE(summary);
  CHECK_EQUAL(render_select(summary->select, PlanDialect{}),
              std::string{
                "SELECT * FROM (SELECT \"key\" AS \"__tenzir_column_0\", "
                "COUNT(*) AS \"__tenzir_column_1\", MAX(\"v\") AS "
                "\"__tenzir_column_2\" FROM \"t\" WHERE (\"v\" > 0) GROUP BY "
                "\"key\") AS \"__tenzir_level_3\" WHERE (\"__tenzir_column_1\" "
                ">= 10) ORDER BY \"__tenzir_column_1\" DESC, "
                "\"__tenzir_column_0\" ASC LIMIT 20"});
  CHECK_EQUAL(layout(summary->layout),
              std::string{"fields=[k=__tenzir_column_0, n=__tenzir_column_1, "
                          "m=__tenzir_column_2] hidden=[] "
                          "schema=tenzir.summarize"});
  // An aggregation ignores the order of its input, unless a limit chose rows
  // by it.
  CHECK_EQUAL(
    sql({sort({"v"}),
         aggregate({{.output = {"n"}, .function = "count", .input = None{}}})}),
    std::string{"SELECT COUNT(*) AS \"__tenzir_column_0\" FROM "
                "\"t\""});
  CHECK_EQUAL(
    sql({sort({"v"}), limit(5),
         aggregate({{.output = {"n"}, .function = "count", .input = None{}}})}),
    std::string{"SELECT COUNT(*) AS \"__tenzir_column_1\" FROM (SELECT * FROM "
                "\"t\" ORDER BY \"v\" ASC LIMIT 5) AS \"__tenzir_level_0\""});
  // A summary without columns yields exactly one empty event, which a
  // hidden count without groups spells, also for empty input.
  auto bare = plan({aggregate(std::vector<ir::AggregateColumn>{})});
  REQUIRE(bare);
  CHECK_EQUAL(render_select(bare->select, PlanDialect{}),
              std::string{"SELECT COUNT(*) AS \"__tenzir_row_0\" FROM \"t\""});
  CHECK_EQUAL(layout(bare->layout),
              std::string{"fields=[] hidden=[__tenzir_row_0] "
                          "schema=tenzir.summarize"});
  // Functions that the dialect does not spell veto the aggregation.
  CHECK(not plan(
    {aggregate({{.output = {"s"}, .function = "sum", .input = Path{"v"}}})}));
}

TEST("plans keep the order of a sort across derived tables") {
  // A filter after a limit needs a derived table, which keeps the sort key in
  // a hidden column, so that the outer query orders by it again.
  auto ordered = plan({sort({"-x"}), limit(5), filter("y > 1")});
  REQUIRE(ordered);
  CHECK_EQUAL(render_select(ordered->select, PlanDialect{}),
              std::string{
                "SELECT * FROM (SELECT *, \"x\" AS \"__tenzir_order_0\" FROM "
                "\"t\" ORDER BY \"x\" DESC LIMIT 5) AS \"__tenzir_level_1\" "
                "WHERE (\"y\" > 1) ORDER BY \"__tenzir_order_0\" DESC"});
  CHECK_EQUAL(layout(ordered->layout),
              std::string{"fields=* hidden=[__tenzir_order_0] schema=-"});
  // The derived table of a selection keeps a sort key that it does not
  // select, and the order survives without a limit, too.
  CHECK_EQUAL(sql({sort({"x"}), select({{{"y"}, {"y"}}}), filter("y > 1")}),
              std::string{
                "SELECT * FROM (SELECT \"y\" AS \"__tenzir_column_0\", \"x\" "
                "AS \"__tenzir_order_1\" FROM \"t\") AS \"__tenzir_level_2\" "
                "WHERE (\"__tenzir_column_0\" > 1) ORDER BY "
                "\"__tenzir_order_1\" ASC"});
  // A level that only passes its input through keeps the fields of the
  // selection before it when its order needs a derived table.
  auto selected
    = plan({select({{{"x"}, {"a"}}}), sort({"x"}), limit(5), filter("x > 1")});
  REQUIRE(selected);
  CHECK_EQUAL(render_select(selected->select, PlanDialect{}),
              std::string{
                "SELECT * FROM (SELECT *, \"__tenzir_column_0\" AS "
                "\"__tenzir_order_2\" FROM (SELECT \"a\" AS "
                "\"__tenzir_column_0\" FROM \"t\") AS \"__tenzir_level_1\" "
                "ORDER BY \"__tenzir_column_0\" ASC LIMIT 5) AS "
                "\"__tenzir_level_3\" WHERE (\"__tenzir_column_0\" > 1) ORDER "
                "BY \"__tenzir_order_2\" ASC"});
  CHECK_EQUAL(layout(selected->layout),
              std::string{"fields=[x=__tenzir_column_0] "
                          "hidden=[__tenzir_order_2] schema=-"});
  // A later sort takes precedence over an earlier one by the same key, which
  // the query names only once.
  CHECK_EQUAL(sql({sort({"x", "y"}), sort({"-x", "-x"}), limit(5)},
                  PlanDialect{LimitClause::top}),
              std::string{"SELECT TOP (5) * FROM \"t\" ORDER BY \"x\" DESC, "
                          "\"y\" ASC"});
  // A sort after a limit sorts the rows that the limit keeps.
  CHECK_EQUAL(sql({limit(5), sort({"x"})}),
              std::string{"SELECT * FROM (SELECT * FROM \"t\" LIMIT 5) AS "
                          "\"__tenzir_level_0\" ORDER BY \"x\" ASC"});
  // `TOP` spells the limit of each level.
  CHECK_EQUAL(sql({sort({"-x"}), limit(5), filter("y > 1")},
                  PlanDialect{LimitClause::top}),
              std::string{
                "SELECT * FROM (SELECT TOP (5) *, \"x\" AS "
                "\"__tenzir_order_0\" FROM \"t\" ORDER BY \"x\" DESC) "
                "AS \"__tenzir_level_1\" WHERE (\"y\" > 1) ORDER BY "
                "\"__tenzir_order_0\" DESC"});
}
