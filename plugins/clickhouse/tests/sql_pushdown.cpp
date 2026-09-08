//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/sql_pushdown.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <string>
#include <string_view>

namespace tenzir::plugins::clickhouse {

namespace {

auto make_schema() -> SqlSchema {
  auto schema = SqlSchema{};
  schema.add_column("id", "UInt64");
  schema.add_column("x", "Int64");
  schema.add_column("y", "String");
  schema.add_column("flag", "Bool");
  schema.add_column("n", "Nullable(Int64)");
  schema.add_column("s", "LowCardinality(Nullable(String))");
  schema.add_column("f", "Float32");
  schema.add_column("status", "Enum8('low' = 1, 'high' = 2)");
  schema.add_column("stamp", "DateTime64(3)");
  schema.add_column("meta", "Tuple(\n    source String,\n    level "
                            "Nullable(Int64),\n    pos Tuple(Int64, Int64))");
  schema.add_column("weird.name", "Int64");
  schema.add_column("gen", "Int64", true);
  return schema;
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

auto translate(std::string_view source) -> Option<std::string> {
  return translate_predicate(parse(source), make_schema());
}

} // namespace

TEST("comparisons against non-nullable columns") {
  CHECK_EQUAL(translate("x > 0"), std::string{"`x` > 0"});
  CHECK_EQUAL(translate("0 < x"), std::string{"`x` > 0"});
  CHECK_EQUAL(translate("x == -3"), std::string{"`x` = -3"});
  CHECK_EQUAL(translate("x != 1.5"), std::string{"`x` != 1.5"});
  CHECK_EQUAL(translate("f <= -0.5"), std::string{"`f` <= -0.5"});
  CHECK_EQUAL(translate("x in [-1, 2]"), std::string{"`x` IN (-1, 2)"});
  CHECK_EQUAL(translate("id >= 18446744073709551615"),
              std::string{"`id` >= 18446744073709551615"});
  CHECK_EQUAL(translate("y == \"foo\""), std::string{"`y` = 'foo'"});
  CHECK_EQUAL(translate("flag == true"), std::string{"`flag` = true"});
  CHECK_EQUAL(translate("flag"), std::string{"`flag`"});
  CHECK_EQUAL(translate("f < 2.5"), std::string{"`f` < 2.5"});
  CHECK_EQUAL(translate("f == 1"), std::string{"`f` = 1"});
  CHECK_EQUAL(translate("x in [1.0, 2.5]"), std::string{"`x` IN (1, 2.5)"});
}

TEST("in requires a homogeneous list") {
  // TQL fixes the element type from the first element and turns clashing
  // elements into `null`, so `x in [1, 2.0]` never matches `2`.
  CHECK(not translate("x in [1, 2.0]"));
  CHECK(not translate("x in [1.0, 2]"));
  CHECK(not translate("f in [1, 2.0]"));
  CHECK(not translate("f in [2.0, 1]"));
  // `18446744073709551615` is a `uint64` literal, `1` is `int64`.
  CHECK(not translate("id in [1, 18446744073709551615]"));
  CHECK_EQUAL(translate("id in [18446744073709551615]"),
              std::string{"`id` IN (18446744073709551615)"});
  CHECK(not translate("y in [\"a\", 1]"));
  CHECK(not translate("flag in [true, 1]"));
  // Negative literals fold to the same type as positive ones.
  CHECK_EQUAL(translate("x in [-1, 2]"), std::string{"`x` IN (-1, 2)"});
  CHECK_EQUAL(translate("f in [-1.5, 2.0]"), std::string{"`f` IN (-1.5, 2)"});
}

TEST("mixed integer and floating-point comparisons stop at 2^53") {
  // TQL compares mixed operands in double, ClickHouse exactly. Below 2^53 the
  // two agree; at and beyond it they may not.
  CHECK_EQUAL(translate("x == 4503599627370496.0"),
              std::string{"`x` = 4503599627370496"});
  CHECK_EQUAL(translate("x > -9007199254740991.0"),
              std::string{"`x` > -9007199254740991"});
  CHECK(not translate("x == 9007199254740992.0"));
  CHECK(not translate("x < -9007199254740992.0"));
  CHECK(not translate("x in [9007199254740992.0]"));
  // An integer literal against a floating column only needs to be exactly
  // representable, which holds for 2^53 and for larger powers of two.
  CHECK_EQUAL(translate("f == 9007199254740992"),
              std::string{"`f` = 9007199254740992"});
  CHECK_EQUAL(translate("f in [1, 1152921504606846976]"),
              std::string{"`f` IN (1, 1152921504606846976)"});
  CHECK(not translate("f >= 9007199254740993"));
  CHECK(not translate("f in [1, 9007199254740993]"));
  CHECK(not translate("f == 18446744073709551615"));
  CHECK(not translate("f == -9223372036854775807"));
  // Same-kind comparisons have no bound.
  CHECK_EQUAL(translate("id >= 18446744073709551615"),
              std::string{"`id` >= 18446744073709551615"});
  CHECK_EQUAL(translate("x in [9007199254740993, -9007199254740993]"),
              std::string{"`x` IN (9007199254740993, -9007199254740993)"});
  CHECK_EQUAL(translate("f > 1e300"), std::string{"`f` > 1e+300"});
}

TEST("nullable columns get a null guard on equality") {
  CHECK_EQUAL(translate("n == 1"),
              std::string{"(`n` IS NOT NULL AND `n` = 1)"});
  CHECK_EQUAL(translate("n != 1"), std::string{"(`n` IS NULL OR `n` != 1)"});
  CHECK_EQUAL(translate("n > 1"), std::string{"`n` > 1"});
  CHECK_EQUAL(translate("n == null"), std::string{"`n` IS NULL"});
  CHECK_EQUAL(translate("n != null"), std::string{"`n` IS NOT NULL"});
  CHECK_EQUAL(translate("s == \"a\""),
              std::string{"(`s` IS NOT NULL AND `s` = 'a')"});
  CHECK_EQUAL(translate("n in [1, 2]"),
              std::string{"(`n` IS NOT NULL AND `n` IN (1, 2))"});
}

TEST("boolean combinators") {
  CHECK_EQUAL(translate("x > 0 or y == \"foo\" and x in [1, 2, 3]"),
              std::string{"(`x` > 0 OR (`y` = 'foo' AND `x` IN (1, 2, 3)))"});
  CHECK_EQUAL(translate("not (x == 1)"), std::string{"NOT `x` = 1"});
  CHECK_EQUAL(translate("not (n == 1)"),
              std::string{"NOT (`n` IS NOT NULL AND `n` = 1)"});
  // One untranslatable side taints the whole disjunction.
  CHECK(not translate("x > 0 or y.starts_with(\"f\")"));
}

TEST("nested tuple elements") {
  CHECK_EQUAL(translate("meta.source == \"a\""),
              std::string{"`meta`.`source` = 'a'"});
  CHECK_EQUAL(translate("meta.level > 2"), std::string{"`meta`.`level` > 2"});
  CHECK_EQUAL(translate("meta.level == 2"),
              std::string{"(`meta`.`level` IS NOT NULL AND `meta`.`level` = "
                          "2)"});
  // Unnamed tuple elements and the tuple itself are not comparable.
  CHECK(not translate("meta.pos == 1"));
  CHECK(not translate("meta == 1"));
  // A top-level column whose name contains a dot is not a nested path.
  CHECK(not translate("weird.name == 1"));
}

TEST("predicates without an exact translation stay local") {
  // Type mismatches yield null in TQL but fail in ClickHouse.
  CHECK(not translate("x == \"foo\""));
  CHECK(not translate("y == 1"));
  CHECK(not translate("flag == 1"));
  // Ordering is only pushed for numbers.
  CHECK(not translate("y < \"b\""));
  CHECK(not translate("flag < true"));
  // Types outside the whitelist.
  CHECK(not translate("status == \"high\""));
  CHECK(not translate("stamp > 2024-01-01T00:00:00"));
  // Generated columns are not part of `SELECT *` by default.
  CHECK(not translate("gen == 1"));
  CHECK(not translate("gen > 1 and x > 1"));
  // Unknown columns, `this`, functions, and column-to-column comparisons.
  CHECK(not translate("nope == 1"));
  CHECK(not translate("this == 1"));
  CHECK(not translate("x.abs() > 1"));
  CHECK(not translate("x == id"));
  // Lists with nulls or non-constants.
  CHECK(not translate("x in [1, null]"));
  CHECK(not translate("x in [id]"));
  CHECK(not translate("x in []"));
}

TEST("conjunctions are split into pushed and remaining parts") {
  auto filter = ir::OptimizeFilter{};
  filter.push_back(parse("x > 0 and y.starts_with(\"f\") and n == null"));
  filter.push_back(parse("flag"));
  auto split = split_filter_for_sql(std::move(filter), make_schema());
  REQUIRE_EQUAL(split.pushed.size(), size_t{3});
  CHECK_EQUAL(split.pushed[0], std::string{"`x` > 0"});
  CHECK_EQUAL(split.pushed[1], std::string{"`n` IS NULL"});
  CHECK_EQUAL(split.pushed[2], std::string{"`flag`"});
  REQUIRE_EQUAL(split.remaining.size(), size_t{1});
  CHECK(is<ast::function_call>(split.remaining[0]));
}

TEST("select query rendering") {
  auto schema = make_schema();
  CHECK_EQUAL(make_select_query("db.tbl", nullptr, None{}, {}, None{}),
              std::string{"SELECT * FROM db.tbl"});
  CHECK_EQUAL(make_select_query("db.tbl", nullptr, None{}, {}, 42),
              std::string{"SELECT * FROM db.tbl LIMIT 42"});
  auto projection = ir::OptimizeProjection{};
  projection.push_back(*ast::field_path::try_from(parse("y")));
  projection.push_back(*ast::field_path::try_from(parse("meta.level")));
  projection.push_back(*ast::field_path::try_from(parse("x")));
  projection.push_back(*ast::field_path::try_from(parse("nope")));
  auto where = std::vector<std::string>{"`x` > 0", "`flag`"};
  // Columns come out in table order; nested paths select their top-level
  // column; unknown fields are skipped.
  CHECK_EQUAL(make_select_query("tbl", &schema, projection, where, 5),
              std::string{"SELECT `x`, `y`, `meta` FROM tbl WHERE `x` > 0 AND "
                          "`flag` LIMIT 5"});
  // A projection without a schema or without surviving columns selects all.
  CHECK_EQUAL(make_select_query("tbl", nullptr, projection, {}, None{}),
              std::string{"SELECT * FROM tbl"});
  auto unknown = ir::OptimizeProjection{};
  unknown.push_back(*ast::field_path::try_from(parse("nope")));
  CHECK_EQUAL(make_select_query("tbl", &schema, unknown, {}, None{}),
              std::string{"SELECT * FROM tbl"});
  // A projection that names a generated column keeps `*`, since whether
  // `SELECT *` includes it depends on session settings.
  auto generated = ir::OptimizeProjection{};
  generated.push_back(*ast::field_path::try_from(parse("id")));
  generated.push_back(*ast::field_path::try_from(parse("gen")));
  CHECK_EQUAL(make_select_query("tbl", &schema, generated, {}, 3),
              std::string{"SELECT * FROM tbl LIMIT 3"});
}

TEST("quoting") {
  CHECK_EQUAL(quote_sql_identifier("plain"), std::string{"`plain`"});
  CHECK_EQUAL(quote_sql_identifier("with`tick"), std::string{"`with\\`tick`"});
  CHECK_EQUAL(quote_sql_identifier("back\\slash"),
              std::string{"`back\\\\slash`"});
  CHECK_EQUAL(quote_sql_string("it's"), std::string{"'it\\'s'"});
  CHECK_EQUAL(quote_sql_string("a\\b"), std::string{"'a\\\\b'"});
  CHECK_EQUAL(quote_sql_string("tab\there"), std::string{"'tab\\x09here'"});
  CHECK_EQUAL(quote_sql_string(std::string_view{"nul\0byte", 8}),
              std::string{"'nul\\x00byte'"});
  CHECK_EQUAL(quote_sql_string("ünïcödé"), std::string{"'ünïcödé'"});
}

} // namespace tenzir::plugins::clickhouse
