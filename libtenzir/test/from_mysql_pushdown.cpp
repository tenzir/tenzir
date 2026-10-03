//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/builtins/from_mysql/pushdown.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/pushdown/translate.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::plugins::mysql;

namespace {

auto text(std::string name, std::string_view type, bool nullable,
          std::string_view charset = "utf8mb4") -> TableColumn {
  auto data_type = std::string{type.substr(0, type.find('('))};
  return TableColumn{
    .name = std::move(name),
    .data_type = std::move(data_type),
    .column_type = std::string{type},
    .nullable = nullable,
    .charset = std::string{charset},
  };
}

auto other(std::string name, std::string_view data_type,
           std::string_view column_type, bool nullable = false) -> TableColumn {
  return TableColumn{
    .name = std::move(name),
    .data_type = std::string{data_type},
    .column_type = std::string{column_type},
    .nullable = nullable,
    .charset = None{},
  };
}

/// The columns of a table, as `from_mysql` reads them from
/// `information_schema.columns`.
auto make_columns() -> std::vector<TableColumn> {
  auto hidden = other("hidden", "int", "int");
  hidden.visible = false;
  return {
    other("id", "int", "int"),
    other("i", "bigint", "bigint", true),
    other("u", "tinyint", "tinyint unsigned"),
    other("ub", "bigint", "bigint unsigned"),
    other("m", "mediumint", "mediumint"),
    other("d", "double", "double"),
    other("n", "double", "double", true),
    other("dp", "double", "double(10,2)"),
    other("f", "float", "float"),
    other("dec", "decimal", "decimal(10,2)"),
    text("s", "varchar(20)", true),
    text("t", "text", false),
    text("c", "char(5)", false),
    text("m3", "varchar(20)", false, "utf8mb3"),
    text("l", "varchar(20)", false, "latin1"),
    text("e", "enum('a','b')", false),
    other("dt", "datetime", "datetime"),
    other("j", "json", "json"),
    other("y", "year", "year"),
    other("bits", "bit", "bit(8)"),
    other("b", "blob", "blob"),
    other("vb", "varbinary", "varbinary(8)"),
    hidden,
  };
}

auto parse(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh);
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ctx));
  return std::move(*expr);
}

auto filter_of(std::initializer_list<std::string_view> sources)
  -> ir::OptimizeFilter {
  auto result = ir::OptimizeFilter{};
  for (auto source : sources) {
    result.push_back(parse(source));
  }
  return result;
}

auto projection_of(std::initializer_list<std::string_view> fields)
  -> Option<ir::OptimizeProjection> {
  auto result = Option<ir::OptimizeProjection>{ir::OptimizeProjection{}};
  for (auto field : fields) {
    auto path = ast::field_path::try_from(parse(field));
    REQUIRE(path);
    ir::add_to_projection(result, std::move(*path));
  }
  return result;
}

/// Renders `expr` against the columns of `make_columns`.
auto render(pushdown::Expr const& expr) -> Option<std::string> {
  auto columns = make_columns();
  return MysqlRenderer{columns}.render(expr);
}

/// Translates and renders `source`, after adapting it to the columns.
auto translate(std::string_view source) -> Option<std::string> {
  auto columns = make_columns();
  auto model = make_column_model(columns);
  auto expr = parse(source);
  pushdown::adapt_to_columns(expr, model);
  auto translated = pushdown::translate_predicate(expr, model);
  if (not translated) {
    return None{};
  }
  return MysqlRenderer{columns}.render(*translated);
}

/// Returns the rendered prefilter of `source`, after adapting it.
auto prefilter(std::string_view source) -> Option<std::string> {
  auto columns = make_columns();
  auto model = make_column_model(columns);
  auto expr = parse(source);
  pushdown::adapt_to_columns(expr, model);
  auto translated = pushdown::translate_prefilter(expr, model);
  if (not translated) {
    return None{};
  }
  return MysqlRenderer{columns}.render(*translated);
}

auto string_equals(std::string literal) -> pushdown::Expr {
  return pushdown::Binary{
    pushdown::BinaryOp::eq,
    pushdown::Expr{pushdown::Column{{"t"}}},
    pushdown::Expr{pushdown::Literal{std::move(literal)}},
  };
}

} // namespace

TEST("column rows parse from information_schema") {
  using row = std::vector<Option<std::string>>;
  auto rows = std::vector<row>{
    {"id", "int", "int unsigned", "NO", None{}, "auto_increment"},
    {"Name", "VARCHAR", "varchar(20)", "YES", "UTF8MB4", ""},
    {"secret", "int", "int", "YES", None{}, "INVISIBLE"},
    {None{}, "int", "int", "YES", None{}, ""},
    {"short", "int"},
  };
  auto columns = parse_table_columns(rows);
  REQUIRE_EQUAL(columns.size(), size_t{3});
  CHECK_EQUAL(columns[0].name, "id");
  CHECK_EQUAL(columns[0].column_type, "int unsigned");
  CHECK(not columns[0].nullable);
  CHECK(not columns[0].charset);
  CHECK(columns[0].visible);
  // Names keep their case; types and character sets do not.
  CHECK_EQUAL(columns[1].name, "Name");
  CHECK_EQUAL(columns[1].data_type, "varchar");
  CHECK_EQUAL(columns[1].charset, Option<std::string>{"utf8mb4"});
  CHECK(columns[1].nullable);
  CHECK(not columns[2].visible);
}

TEST("column types map to what TQL sees") {
  auto columns = make_columns();
  auto find = [&](std::string_view name) -> TableColumn const& {
    auto it = std::ranges::find(columns, name, &TableColumn::name);
    REQUIRE(it != columns.end());
    return *it;
  };
  auto integer = [&](std::string_view name) {
    auto type = parse_column_type(find(name));
    REQUIRE(type);
    auto const* result = try_as<pushdown::IntType>(*type);
    REQUIRE(result);
    return *result;
  };
  CHECK_EQUAL(integer("id").bits, 32);
  CHECK(integer("id").is_signed);
  CHECK_EQUAL(integer("u").bits, 8);
  CHECK(not integer("u").is_signed);
  CHECK(not integer("ub").is_signed);
  CHECK_EQUAL(integer("m").bits, 24);
  CHECK(is<pushdown::FloatType>(*parse_column_type(find("d"))));
  CHECK(is<pushdown::StringType>(*parse_column_type(find("s"))));
  CHECK(is<pushdown::StringType>(*parse_column_type(find("t"))));
  CHECK(is<pushdown::StringType>(*parse_column_type(find("c"))));
  CHECK(is<pushdown::StringType>(*parse_column_type(find("m3"))));
  for (auto name :
       {"dp", "f", "dec", "l", "e", "dt", "j", "y", "bits", "b", "vb"}) {
    CHECK_EQUAL(fmt::format("{}: {}", name,
                            parse_column_type(find(name)).has_value()),
                fmt::format("{}: false", name));
  }
  // Invisible columns are absent from `SELECT *`, so TQL never sees them.
  auto model = make_column_model(columns);
  CHECK(not model.find({"hidden"}));
  CHECK(model.find({"id"}));
}

TEST("comparisons translate to MySQL") {
  CHECK_EQUAL(translate("id > 1"), "`id` > 1");
  CHECK_EQUAL(translate("1 < id"), "`id` > 1");
  CHECK_EQUAL(translate("i == 1"), "(`i` IS NOT NULL AND `i` = 1)");
  CHECK_EQUAL(translate("i != 1"), "(`i` IS NULL OR `i` != 1)");
  CHECK_EQUAL(translate("i == null"), "`i` IS NULL");
  CHECK_EQUAL(translate("ub == 18446744073709551615"),
              "`ub` = 18446744073709551615");
  CHECK_EQUAL(translate("u >= -1"), "`u` >= -1");
  CHECK_EQUAL(translate("id in [1, 2]"), "`id` IN (1, 2)");
  CHECK_EQUAL(translate("i < ub"), "`i` < `ub`");
  // Doubles carry an exponent so that MySQL reads them as `DOUBLE`.
  CHECK_EQUAL(translate("d > 0.5"), "`d` > 0.5e0");
  CHECK_EQUAL(translate("d == 1e300"), "`d` = 1e+300");
  CHECK_EQUAL(translate("id == 1.0"), "`id` = 1e0");
  CHECK_EQUAL(translate("d < n"), "`d` < `n`");
  // An integer literal against a `DOUBLE` column is a `double`, too.
  CHECK_EQUAL(translate("d > 1"), "`d` > 1e0");
  CHECK_EQUAL(translate("not (id > 1 or d < 0)"),
              "NOT (`id` > 1 OR `d` < 0e0)");
}

TEST("strings compare as binary strings") {
  CHECK_EQUAL(translate(R"(t == "a")"), "CAST(`t` AS BINARY) = _binary'a'");
  CHECK_EQUAL(translate(R"(s == "a")"),
              "(CAST(`s` AS BINARY) IS NOT NULL AND CAST(`s` AS BINARY) = "
              "_binary'a')");
  CHECK_EQUAL(translate(R"(c < "b")"), "CAST(`c` AS BINARY) < _binary'b'");
  CHECK_EQUAL(translate(R"(m3 >= "")"), "CAST(`m3` AS BINARY) >= _binary''");
  CHECK_EQUAL(translate(R"(t in ["a", "b"])"),
              "CAST(`t` AS BINARY) IN (_binary'a', _binary'b')");
  CHECK_EQUAL(translate("t == m3"),
              "CAST(`t` AS BINARY) = CAST(`m3` AS BINARY)");
  CHECK_EQUAL(translate(R"(t == "it's")"),
              "CAST(`t` AS BINARY) = _binary'it''s'");
  // Backslashes and NUL bytes take a hex literal, which means the same with
  // and without `NO_BACKSLASH_ESCAPES`.
  CHECK_EQUAL(translate(R"(t == "a\\b")"), "CAST(`t` AS BINARY) = X'615C62'");
  CHECK_EQUAL(render(string_equals(std::string{"a\0b", 3})),
              "CAST(`t` AS BINARY) = X'610062'");
  CHECK_EQUAL(render(string_equals("é")), "CAST(`t` AS BINARY) = _binary'é'");
  CHECK_EQUAL(render(string_equals("\xff")), None{});
  // An `ip` literal against a string column compares its text.
  CHECK_EQUAL(translate("t == 1.2.3.4"),
              "CAST(`t` AS BINARY) = _binary'1.2.3.4'");
}

TEST("string functions count bytes") {
  CHECK_EQUAL(translate(R"(t.starts_with("ab"))"),
              "LEFT(CAST(`t` AS BINARY), LENGTH(_binary'ab')) = _binary'ab'");
  CHECK_EQUAL(translate(R"(t.ends_with("ab"))"),
              "RIGHT(CAST(`t` AS BINARY), LENGTH(_binary'ab')) = "
              "_binary'ab'");
  CHECK_EQUAL(translate(R"("ab" in t)"),
              "INSTR(CAST(`t` AS BINARY), _binary'ab') > 0");
  CHECK_EQUAL(translate("t.length_bytes() > 2"),
              "LENGTH(CAST(`t` AS BINARY)) > 2");
  CHECK_EQUAL(translate(R"(s.starts_with("a") and id > 0)"),
              "(LEFT(CAST(`s` AS BINARY), LENGTH(_binary'a')) = _binary'a' "
              "AND `id` > 0)");
  auto folded = std::string_view{
    "CAST(LOWER(CAST({} AS CHAR CHARACTER SET utf8mb4)) AS BINARY)"};
  auto column = fmt::format(fmt::runtime(folded), "CAST(`t` AS BINARY)");
  auto literal = fmt::format(fmt::runtime(folded), "_binary'A'");
  CHECK_EQUAL(translate(R"(t.starts_with("A", ignore_case=true))"),
              fmt::format("LEFT({}, LENGTH({})) = {}", column, literal,
                          literal));
  CHECK_EQUAL(translate("t.length_chars() > 2"), None{});
  // MySQL matches regular expressions with ICU, not RE2.
  CHECK_EQUAL(translate(R"(t.match_regex("^a"))"), None{});
  // MySQL has no address type to compare a parsed address with.
  CHECK_EQUAL(prefilter("t in 10.0.0.0/8"), None{});
}

TEST("unsupported columns and arithmetic stay local") {
  for (auto source :
       {"f > 1", "dec > 1", "dp > 1", R"(l == "a")", R"(e == "a")",
        R"(dt == "2024-01-01")", R"(j == "[]")", "y == 2024", "hidden == 1",
        "missing == 1", "id + 1 > 2", "d * 2.0 > 1", "id / 2 > 1"}) {
    CHECK_EQUAL(fmt::format("{}: {}", source, translate(source)),
                fmt::format("{}: {}", source, Option<std::string>{}));
  }
}

TEST("plans push what translates and select what remains") {
  auto columns = make_columns();
  // Everything translates: the filter's columns need not be selected.
  auto plan = make_table_plan(columns, filter_of({"id > 0", R"(t == "x")"}),
                              projection_of({"s", "id"}));
  CHECK_EQUAL(plan.selection, "`id`, `s`");
  CHECK_EQUAL(fmt::to_string(fmt::join(plan.pushed, " AND ")),
              "`id` > 0 AND CAST(`t` AS BINARY) = _binary'x'");
  CHECK(plan.local.empty());
  CHECK_EQUAL(make_table_query(plan, "tbl", 10),
              "SELECT `id`, `s` FROM `tbl` WHERE `id` > 0 AND CAST(`t` AS "
              "BINARY) = _binary'x' LIMIT 10");
  // A conjunct without translation stays local, keeps its column, and takes
  // the limit with it.
  plan = make_table_plan(columns, filter_of({"id > 0 and f > 1"}),
                         projection_of({"s"}));
  CHECK_EQUAL(plan.selection, "`f`, `s`");
  CHECK_EQUAL(fmt::to_string(fmt::join(plan.pushed, " AND ")), "`id` > 0");
  REQUIRE_EQUAL(plan.local.size(), size_t{1});
  CHECK_EQUAL(make_table_query(plan, "tbl", 10),
              "SELECT `f`, `s` FROM `tbl` WHERE `id` > 0");
  // A local function call may look at any field.
  plan = make_table_plan(columns, filter_of({"t.to_upper() == \"X\""}),
                         projection_of({"s"}));
  CHECK_EQUAL(plan.selection, "*");
  CHECK(plan.pushed.empty());
  // Without a projection, downstream needs every field.
  plan = make_table_plan(columns, filter_of({"id > 0"}), None{});
  CHECK_EQUAL(plan.selection, "*");
  CHECK_EQUAL(make_table_query(plan, "tbl", None{}),
              "SELECT * FROM `tbl` WHERE `id` > 0");
  // MySQL accepts every `uint64` limit.
  CHECK_EQUAL(make_table_query(plan, "tbl", 18446744073709551615u),
              "SELECT * FROM `tbl` WHERE `id` > 0 LIMIT 18446744073709551615");
}

TEST("projections keep exact names and rows") {
  auto columns = make_columns();
  // Only exact names select a column: MySQL would label `ID` as written.
  auto plan
    = make_table_plan(columns, {}, projection_of({"ID", "missing", "t.x"}));
  CHECK_EQUAL(plan.selection, "`t`");
  // An empty projection still needs the rows, so it selects the first column.
  plan = make_table_plan(columns, {}, ir::OptimizeProjection{});
  CHECK_EQUAL(plan.selection, "`id`");
  plan = make_table_plan(columns, {}, projection_of({"missing"}));
  CHECK_EQUAL(plan.selection, "`id`");
  // `SELECT *` leaves out invisible columns, so a projection does, too.
  plan = make_table_plan(columns, {}, projection_of({"hidden", "id"}));
  CHECK_EQUAL(plan.selection, "`id`");
  plan = make_table_plan(columns, {}, projection_of({"this"}));
  CHECK_EQUAL(plan.selection, "*");
  // Identifiers escape backticks.
  auto odd = std::vector<TableColumn>{other("a`b", "int", "int")};
  plan = make_table_plan(odd, filter_of({"this[\"a`b\"] > 1"}),
                         projection_of({"this[\"a`b\"]"}));
  CHECK_EQUAL(plan.selection, "`a``b`");
  CHECK_EQUAL(fmt::to_string(fmt::join(plan.pushed, " AND ")), "`a``b` > 1");
}

TEST("unknown tables leave everything local") {
  auto plan = make_table_plan({}, filter_of({"id > 0"}), projection_of({"id"}));
  CHECK_EQUAL(plan.selection, "*");
  CHECK(plan.pushed.empty());
  CHECK_EQUAL(plan.local.size(), size_t{1});
  CHECK_EQUAL(make_table_query(plan, "tbl", 5), "SELECT * FROM `tbl`");
}
