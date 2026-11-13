//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/sql_pushdown.hpp"

#include "clickhouse/block_decoding.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/registry.hpp>
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
  schema.add_column("meta", "Tuple(\n    source String,\n    level "
                            "Nullable(Int64),\n    pos Tuple(Int64, Int64))");
  schema.add_column("gen", "Int64", true);
  return schema;
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

} // namespace

TEST("column selection") {
  auto schema = make_schema();
  CHECK_EQUAL(select_columns(nullptr, None{}), std::string{"*"});
  CHECK_EQUAL(select_columns(&schema, None{}), std::string{"*"});
  auto projection = ir::OptimizeProjection{};
  projection.push_back(*ast::field_path::try_from(parse("y")));
  projection.push_back(*ast::field_path::try_from(parse("meta.level")));
  projection.push_back(*ast::field_path::try_from(parse("x")));
  projection.push_back(*ast::field_path::try_from(parse("nope")));
  // Columns come out in table order; a nested path narrows its top-level
  // column to the requested tuple elements; unknown fields are skipped.
  CHECK_EQUAL(select_columns(&schema, projection),
              std::string{"`x`, `y`, CAST(tuple(tupleElement(`meta`, "
                          "'level')), 'Tuple(`level` Nullable(Int64))') AS "
                          "`meta`"});
  // A projection without a schema or without surviving columns selects all.
  CHECK_EQUAL(select_columns(nullptr, projection), std::string{"*"});
  auto unknown = ir::OptimizeProjection{};
  unknown.push_back(*ast::field_path::try_from(parse("nope")));
  CHECK_EQUAL(select_columns(&schema, unknown), std::string{"*"});
  // A projection that names a generated column keeps `*`, since whether
  // `SELECT *` includes it depends on session settings.
  auto generated = ir::OptimizeProjection{};
  generated.push_back(*ast::field_path::try_from(parse("id")));
  generated.push_back(*ast::field_path::try_from(parse("gen")));
  CHECK_EQUAL(select_columns(&schema, generated), std::string{"*"});
}

TEST("nested projections") {
  auto schema = SqlSchema{};
  schema.add_column("id", "UInt64");
  schema.add_column("m",
                    "Tuple(a String, b Nullable(Int64), c Tuple(d IPv4, "
                    "e LowCardinality(String)), `f g` Bool, h Tuple(Int64))");
  auto select = [&](std::initializer_list<std::string_view> fields) {
    auto projection = ir::OptimizeProjection{};
    for (auto field : fields) {
      projection.push_back(*ast::field_path::try_from(parse(field)));
    }
    return select_columns(&schema, projection);
  };
  // Elements come out in declaration order, nested tuples are rebuilt
  // recursively, and names are quoted.
  CHECK_EQUAL(select({"m.c.e", "m.b", "id"}),
              std::string{"`id`, CAST(tuple(tupleElement(`m`, 'b'), "
                          "CAST(tuple(tupleElement(tupleElement(`m`, 'c'), "
                          "'e')), 'Tuple(`e` LowCardinality(String))')), "
                          "'Tuple(`b` "
                          "Nullable(Int64), `c` Tuple(`e` "
                          "LowCardinality(String)))') AS `m`"});
  CHECK_EQUAL(select({"m.c", "m.a"}),
              std::string{"CAST(tuple(tupleElement(`m`, 'a'), "
                          "tupleElement(`m`, 'c')), 'Tuple(`a` "
                          "String, `c` Tuple(d IPv4,e "
                          "LowCardinality(String)))') AS `m`"});
  CHECK_EQUAL(select({"m[\"f g\"]"}),
              std::string{"CAST(tuple(tupleElement(`m`, 'f g')), "
                          "'Tuple(`f g` "
                          "Bool)') AS `m`"});
  // The whole column, a path below it, or a path outside the schema select
  // the column as is.
  CHECK_EQUAL(select({"m", "m.a"}), std::string{"`m`"});
  CHECK_EQUAL(select({"m.a", "m.nope"}), std::string{"`m`"});
  CHECK_EQUAL(select({"m.h"}),
              std::string{"CAST(tuple(tupleElement(`m`, 'h')), "
                          "'Tuple(`h` "
                          "Tuple(Int64))') AS `m`"});
  CHECK_EQUAL(select({"id.nope"}), std::string{"`id`"});
  // A map and a tuple that contains one stay whole: the fields of a map depend
  // on the data, and only the events decoder reads them.
  schema.add_column("mp", "Map(String, String)");
  CHECK_EQUAL(select({"mp.k"}), std::string{"`mp`"});
  schema.add_column("half", "Tuple(ok String, bad Map(String, Int64))");
  CHECK_EQUAL(select({"half.ok"}), std::string{"`half`"});
  CHECK_EQUAL(select({"m.a", "half.ok"}),
              std::string{"CAST(tuple(tupleElement(`m`, 'a')), "
                          "'Tuple(`a` String)') AS `m`, `half`"});
}

TEST("decodable types") {
  CHECK(is_decodable_type("Int64"));
  CHECK(is_decodable_type("LowCardinality(Nullable(String))"));
  CHECK(is_decodable_type("Tuple(a String,b Tuple(c IPv4))"));
  CHECK(is_decodable_type("Array(UInt8)"));
  CHECK(is_decodable_type("Decimal(10, 2)"));
  CHECK(is_decodable_type("Enum8('a' = 1)"));
  CHECK(is_decodable_type("DateTime64(3, 'UTC')"));
  // Maps decode with the events decoder only, and pushdown leaves them alone.
  CHECK(not is_decodable_type("Map(String, Int64)"));
  CHECK(not is_decodable_type("Tuple(a String, b Map(String, Int64))"));
  CHECK(not is_decodable_type("Array(Map(String, Int64))"));
  CHECK(not is_decodable_type("Nonsense"));
  CHECK(not is_decodable_type(""));
}

TEST("quoting") {
  CHECK_EQUAL(quote_sql_identifier("plain"), std::string{"`plain`"});
  CHECK_EQUAL(quote_sql_identifier("with`tick"), std::string{"`with\\`tick`"});
  CHECK_EQUAL(quote_sql_identifier("back\\slash"),
              std::string{"`back\\\\slash`"});
  CHECK_EQUAL(quote_sql_string("it's"), std::string{"'it\\'s'"});
  CHECK_EQUAL(quote_sql_string("a\\b"), std::string{"'a\\\\b'"});
  CHECK_EQUAL(quote_sql_string("tab\there"), std::string{"'tab\\x09here'"});
  CHECK_EQUAL(quote_sql_string("esc\x1b\x7f"), std::string{"'esc\\x1B\\x7F'"});
  CHECK_EQUAL(quote_sql_string(std::string_view{"nul\0byte", 8}),
              std::string{"'nul\\x00byte'"});
  CHECK_EQUAL(quote_sql_string("ünïcödé"), std::string{"'ünïcödé'"});
}

} // namespace tenzir::plugins::clickhouse
