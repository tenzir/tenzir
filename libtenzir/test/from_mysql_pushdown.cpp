//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/builtins/from_mysql/pushdown.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/session.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/try.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <limits>
#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;
using namespace tenzir::plugins::mysql;

namespace {

using Path = std::vector<std::string>;

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

/// The query that reads the table `t` with `stages`.
auto try_read(std::vector<ir::QueryStage> stages,
              std::vector<std::string> columns = {"id", "s"})
  -> Result<std::string, std::string> {
  TRY(auto read, pushdown::make_table_read("`t`", "*", {}, stages, columns,
                                           MysqlDialect{}));
  return std::move(read.sql);
}

auto read(std::vector<ir::QueryStage> stages) -> std::string {
  auto result = try_read(std::move(stages));
  REQUIRE(result);
  return std::move(result).unwrap();
}

auto prefix(std::vector<ir::QueryStage> stages, bool live = false)
  -> ir::Absorption {
  return absorb_stages(ir::QueryChain{.stages = std::move(stages), .end = ""},
                       live);
}

} // namespace

TEST("mysql column rows parse from information_schema") {
  auto rows = std::vector<std::vector<Option<std::string>>>{
    {"id", "auto_increment"},
    {"hidden", "INVISIBLE"},
    {"s", None{}},
    {None{}, ""},
    {"short"},
  };
  auto columns = parse_table_columns(rows);
  REQUIRE_EQUAL(columns.size(), size_t{3});
  CHECK_EQUAL(columns[0].name, "id");
  CHECK(columns[0].visible);
  CHECK(not columns[1].visible);
  CHECK(columns[2].visible);
  CHECK_EQUAL(column_names(columns),
              (std::vector<std::string>{"id", "hidden", "s"}));
}

TEST("mysql spells filters, selections, sorts, and limits") {
  CHECK_EQUAL(
    read({filter("id > 1 and s.starts_with(\"a\")"), sort({"-id"}), limit(5)}),
    std::string{"SELECT * FROM `t` WHERE ((`id` > 1) AND "
                "(LEFT(`s`, CHAR_LENGTH('a')) = 'a')) ORDER BY `id` "
                "DESC LIMIT 5"});
  CHECK_EQUAL(read({select({{{"x"}, {"id"}}}), filter("x < 3")}),
              std::string{"SELECT * FROM (SELECT `id` AS `__tenzir_column_0` "
                          "FROM `t`) AS `__tenzir_level_1` WHERE "
                          "(`__tenzir_column_0` < 3)"});
  // MySQL needs `*` next to other columns qualified with its relation.
  CHECK_EQUAL(read({sort({"-id"}), limit(3), filter("id > 1")}),
              std::string{"SELECT * FROM (SELECT `__tenzir_source_1`.*, `id` "
                          "AS `__tenzir_order_0` FROM `t` AS "
                          "`__tenzir_source_1` ORDER BY `id` DESC LIMIT 3) AS "
                          "`__tenzir_level_2` WHERE (`id` > 1) ORDER BY "
                          "`__tenzir_order_0` DESC"});
  // Names that the query introduces must not name a column, in any case.
  auto clash
    = try_read({select({{{"x"}, {"id"}}})}, {"id", "__TENZIR_COLUMN_0"});
  REQUIRE(not clash);
}

TEST("mysql spells literals in every SQL mode") {
  // Times are in UTC with microseconds.
  CHECK_EQUAL(read({filter("ts >= 2024-01-01T00:00:00.000001")}),
              std::string{"SELECT * FROM `t` WHERE (`ts` >= '2024-01-01 "
                          "00:00:00.000001')"});
  CHECK_EQUAL(read({filter("ts < 1969-12-31T23:59:59.5")}),
              std::string{"SELECT * FROM `t` WHERE (`ts` < '1969-12-31 "
                          "23:59:59.500000')"});
  // IP addresses become text, and quotes double.
  CHECK_EQUAL(read({filter("ip == 10.0.0.1 or s == \"it's\"")}),
              std::string{"SELECT * FROM `t` WHERE ((`ip` = '10.0.0.1') OR "
                          "(`s` = 'it''s'))"});
  // Doubles carry an exponent, so that MySQL reads them as `DOUBLE`.
  CHECK_EQUAL(read({filter("x > 0.5")}),
              std::string{"SELECT * FROM `t` WHERE (`x` > 0.5e0)"});
  // Functions map to MySQL's.
  CHECK_EQUAL(read({filter("s.length_chars() > 1 and s.to_lower() == \"a\"")}),
              std::string{"SELECT * FROM `t` WHERE ((CHAR_LENGTH(`s`) > 1) "
                          "AND (LOWER(`s`) = 'a'))"});
  CHECK_EQUAL(read({filter("s.match_regex(\"^a\")")}),
              std::string{"SELECT * FROM `t` WHERE REGEXP_LIKE(`s`, '^a', "
                          "'c')"});
}

TEST("mysql leaves what it cannot spell in the pipeline") {
  // A backslash depends on the SQL mode, and NUL bytes end the query.
  CHECK_EQUAL(prefix({filter("s == \"a\\\\b\"")}).prefix, size_t{0});
  CHECK_EQUAL(prefix({filter("s == \"a\\x00b\"")}).prefix, size_t{0});
  // MySQL keeps microseconds and has no interval values.
  CHECK_EQUAL(prefix({filter("ts > 2024-01-01T00:00:00.000000001")}).prefix,
              size_t{0});
  CHECK_EQUAL(prefix({filter("d > 1s")}).prefix, size_t{0});
  CHECK_EQUAL(prefix({filter("ts - 2024-01-01 > 1")}).declined,
              std::string{"MySQL cannot compute with a time literal"});
  // Nested fields, subnets, other functions, and a drop of columns that the
  // plan does not know stay, too.
  CHECK_EQUAL(prefix({filter("a.b == 1")}).prefix, size_t{0});
  CHECK_EQUAL(prefix({filter("ip in 10.0.0.0/8")}).prefix, size_t{0});
  CHECK_EQUAL(prefix({filter("s.capitalize() == \"A\"")}).prefix, size_t{0});
  CHECK_EQUAL(prefix({drop({{"s"}})}).prefix, size_t{0});
  auto partial
    = prefix({filter("id > 1"), filter("s.capitalize() == \"A\""), limit(1)});
  CHECK_EQUAL(partial.prefix, size_t{1});
  CHECK_EQUAL(partial.declined, std::string{"MySQL cannot spell the stage"});
  // A drop after a selection knows its columns.
  CHECK_EQUAL(
    prefix({select({{{"a"}, {"id"}}, {{"b"}, {"s"}}}), drop({{"b"}})}).prefix,
    size_t{2});
  // A live read takes over filters only.
  auto live = prefix({filter("id > 1"), sort({"id"}), limit(1)}, true);
  CHECK_EQUAL(live.prefix, size_t{1});
  CHECK_EQUAL(live.declined,
              std::string{"a live read takes over filters only"});
  CHECK_EQUAL(prefix({limit(std::numeric_limits<uint64_t>::max())}).prefix,
              size_t{1});
}

TEST("mysql reads windows of live reads") {
  auto sql = pushdown::make_window_read(
    "`t`", "`s`", std::vector<std::string>{"`id` <= 9", "`id` > 3"},
    std::vector{filter("s != \"x\"")}, "id", MysqlDialect{});
  REQUIRE(sql);
  CHECK_EQUAL(sql.unwrap(),
              std::string{"SELECT `s` FROM `t` WHERE `id` <= 9 AND `id` > 3 "
                          "AND (`s` <> 'x') ORDER BY `id` ASC"});
}

TEST("mysql logs generated queries without literals") {
  CHECK_EQUAL(pushdown::redact_query("SELECT * FROM `t` WHERE `s` = 'secret' "
                                     "# 'x'",
                                     mysql_lexicon),
              std::string{"SELECT * FROM `t` WHERE `s` = '***' # ***"});
}

TEST("mysql selects invisible columns only for the stages") {
  auto const columns = std::vector<TableColumn>{
    {.name = "h", .visible = false},
    {.name = "id", .visible = true},
    {.name = "g", .visible = false},
  };
  auto const projection = [](std::vector<std::string_view> fields) {
    auto result = ir::OptimizeProjection{};
    for (auto field : fields) {
      result.push_back(*ast::field_path::try_from(parse(field)));
    }
    return Option<ir::OptimizeProjection>{std::move(result)};
  };
  auto const select = [&](Option<ir::OptimizeProjection> const& projected,
                          std::vector<ir::QueryStage> plan = {}) {
    auto result = select_columns(projected, columns, {}, plan, "`t`");
    return fmt::format("{} | extra={}", result.selection,
                       fmt::join(result.extra, ","));
  };
  // A projection keeps what `SELECT *` yields, so an invisible column that
  // only downstream names stays out, and a read without fields keeps the
  // first visible column.
  CHECK_EQUAL(select(projection({"id", "H"})), std::string{"`id` | extra="});
  CHECK_EQUAL(select(projection({"H"})), std::string{"`id` | extra="});
  CHECK_EQUAL(select(None{}), std::string{"* | extra="});
  // The stages may read invisible columns, which the result leaves out.
  CHECK_EQUAL(select(None{}, {limit(2), filter("g > 1")}),
              std::string{"`t`.*, `g` | extra=g"});
  CHECK_EQUAL(select(projection({"id", "g"}), {limit(2), filter("g > 1")}),
              std::string{"`id`, `g` | extra=g"});
  CHECK_EQUAL(select(None{}, {limit(2)}), std::string{"* | extra="});
}
