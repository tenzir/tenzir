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
    .limit = None{},
  };
  CHECK_EQUAL(render_select(select, TestDialect{}), "SELECT * FROM t");
  select = SqlSelect{
    .selection = "a, count(*)",
    .from = "t",
    .where = {"x > 0", "y < 1"},
    .group_by = {"a", "b"},
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
