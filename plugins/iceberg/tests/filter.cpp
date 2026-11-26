//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/filter.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/session.hpp>
#include <tenzir/test/registry.hpp>
#include <tenzir/test/test.hpp>
#include <tenzir/time.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/parser.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <iceberg/expression/expression.h>
#include <iceberg/expression/expressions.h>
#include <iceberg/expression/inclusive_metrics_evaluator.h>
#include <iceberg/expression/literal.h>
#include <iceberg/expression/manifest_evaluator.h>
#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/manifest/manifest_list.h>
#include <iceberg/partition_field.h>
#include <iceberg/partition_spec.h>
#include <iceberg/schema.h>
#include <iceberg/schema_field.h>
#include <iceberg/transform.h>
#include <iceberg/type.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

namespace ice = ::iceberg;

// Field IDs for the shared test schema.
constexpr auto id_col = int32_t{1};
constexpr auto i32_col = int32_t{2};
constexpr auto flag_col = int32_t{3};
constexpr auto f32_col = int32_t{4};
constexpr auto f64_col = int32_t{5};
constexpr auto name_col = int32_t{6};
constexpr auto ts_col = int32_t{7};
constexpr auto ts_tz_col = int32_t{8};
constexpr auto ts_ns_col = int32_t{9};
constexpr auto ts_tz_ns_col = int32_t{10};
constexpr auto day_col = int32_t{11};
constexpr auto tod_col = int32_t{12};
constexpr auto dec_col = int32_t{13};
constexpr auto nested_col = int32_t{14};
constexpr auto nested_a_col = int32_t{15};
constexpr auto nested_b_col = int32_t{16};
constexpr auto nested_inner_col = int32_t{17};
constexpr auto nested_inner_x_col = int32_t{18};
constexpr auto labels_col = int32_t{19};
constexpr auto labels_key_col = int32_t{20};
constexpr auto labels_val_col = int32_t{21};
constexpr auto items_col = int32_t{22};
constexpr auto items_elem_col = int32_t{23};
constexpr auto items_k_col = int32_t{24};
constexpr auto payload_col = int32_t{25};

auto make_test_schema() -> ice::Schema {
  auto inner_struct = ice::struct_({
    ice::SchemaField::MakeOptional(nested_inner_x_col, "x", ice::int32()),
  });
  auto nested_struct = ice::struct_({
    ice::SchemaField::MakeOptional(nested_a_col, "a", ice::int64()),
    ice::SchemaField::MakeOptional(nested_b_col, "b", ice::string()),
    ice::SchemaField::MakeOptional(nested_inner_col, "inner",
                                   std::move(inner_struct)),
  });
  auto labels_map = ice::map(
    ice::SchemaField::MakeRequired(labels_key_col, "key", ice::string()),
    ice::SchemaField::MakeOptional(labels_val_col, "value", ice::string()));
  auto item_struct = ice::struct_({
    ice::SchemaField::MakeOptional(items_k_col, "k", ice::int64()),
  });
  auto items_list = ice::list(ice::SchemaField::MakeOptional(
    items_elem_col, "element", std::move(item_struct)));
  return ice::Schema{
    std::vector<ice::SchemaField>{
      ice::SchemaField::MakeOptional(id_col, "id", ice::int64()),
      ice::SchemaField::MakeOptional(i32_col, "i32", ice::int32()),
      ice::SchemaField::MakeOptional(flag_col, "flag", ice::boolean()),
      ice::SchemaField::MakeOptional(f32_col, "f32", ice::float32()),
      ice::SchemaField::MakeOptional(f64_col, "f64", ice::float64()),
      ice::SchemaField::MakeOptional(name_col, "name", ice::string()),
      ice::SchemaField::MakeOptional(ts_col, "ts", ice::timestamp()),
      ice::SchemaField::MakeOptional(ts_tz_col, "ts_tz", ice::timestamp_tz()),
      ice::SchemaField::MakeOptional(ts_ns_col, "ts_ns", ice::timestamp_ns()),
      ice::SchemaField::MakeOptional(ts_tz_ns_col, "ts_tz_ns",
                                     ice::timestamptz_ns()),
      ice::SchemaField::MakeOptional(day_col, "day", ice::date()),
      ice::SchemaField::MakeOptional(tod_col, "tod", ice::time()),
      ice::SchemaField::MakeOptional(dec_col, "dec", ice::decimal(10, 2)),
      ice::SchemaField::MakeOptional(nested_col, "nested",
                                     std::move(nested_struct)),
      ice::SchemaField::MakeOptional(labels_col, "labels",
                                     std::move(labels_map)),
      ice::SchemaField::MakeOptional(items_col, "items", std::move(items_list)),
      ice::SchemaField::MakeOptional(payload_col, "payload", ice::variant()),
    },
    0,
  };
}

auto parse_tql(std::string_view source) -> ast::expression {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, test::builtin_registry());
  auto ctx = provider.as_session();
  auto expr
    = parse_expression_with_location_override(source, location::unknown, ctx);
  REQUIRE(expr);
  REQUIRE(resolve_entities(*expr, ExternPrelude::empty(), ctx));
  return std::move(*expr);
}

auto translate(std::string_view source, ice::Schema const& schema)
  -> std::shared_ptr<ice::Expression> {
  return translate_expression(parse_tql(source), schema);
}

auto serialize(ice::Literal const& lit) -> std::vector<uint8_t> {
  auto bytes = lit.Serialize();
  REQUIRE(bytes.has_value());
  return *std::move(bytes);
}

auto epoch_days(int year, unsigned month, unsigned day) -> int32_t {
  auto d = sys_days{std::chrono::year{year} / std::chrono::month{month}
                    / std::chrono::day{day}};
  return static_cast<int32_t>(d.time_since_epoch().count());
}

auto epoch_us(int year, unsigned month, unsigned day, int hour = 0,
              int minute = 0, int second = 0, int64_t extra_us = 0) -> int64_t {
  auto d = sys_days{std::chrono::year{year} / std::chrono::month{month}
                    / std::chrono::day{day}};
  auto tp = d + std::chrono::hours{hour} + std::chrono::minutes{minute}
            + std::chrono::seconds{second}
            + std::chrono::microseconds{extra_us};
  return std::chrono::duration_cast<std::chrono::microseconds>(
           tp.time_since_epoch())
    .count();
}

auto make_data_file(int32_t field_id, ice::Literal const& lower,
                    ice::Literal const& upper, int64_t null_count = 0,
                    int64_t record_count = 100) -> ice::DataFile {
  auto file = ice::DataFile{};
  file.record_count = record_count;
  file.value_counts[field_id] = record_count;
  file.null_value_counts[field_id] = null_count;
  file.lower_bounds[field_id] = serialize(lower);
  file.upper_bounds[field_id] = serialize(upper);
  return file;
}

auto make_all_null_file(int32_t field_id, int64_t record_count = 100)
  -> ice::DataFile {
  auto file = ice::DataFile{};
  file.record_count = record_count;
  file.value_counts[field_id] = record_count;
  file.null_value_counts[field_id] = record_count;
  return file;
}

auto eval_metrics(std::shared_ptr<ice::Expression> expr,
                  ice::Schema const& schema, ice::DataFile const& file)
  -> bool {
  auto evaluator = ice::InclusiveMetricsEvaluator::Make(
    std::move(expr), schema, /*case_sensitive=*/true);
  REQUIRE(evaluator.has_value());
  auto result = (*evaluator)->Evaluate(file);
  REQUIRE(result.has_value());
  return *result;
}

auto eval_manifest(std::shared_ptr<ice::Expression> expr,
                   std::shared_ptr<ice::PartitionSpec> const& spec,
                   ice::Schema const& schema, ice::ManifestFile const& manifest)
  -> bool {
  auto evaluator = ice::ManifestEvaluator::MakeRowFilter(
    std::move(expr), spec, schema, /*case_sensitive=*/true);
  REQUIRE(evaluator.has_value());
  auto result = (*evaluator)->Evaluate(manifest);
  REQUIRE(result.has_value());
  return *result;
}

TEST("translate_filter combines predicates and folds constants") {
  auto schema = make_test_schema();
  // Empty filter stays AlwaysTrue.
  auto empty = translate_filter(ir::OptimizeFilter{}, schema);
  CHECK(empty->Equals(*ice::Expressions::AlwaysTrue()));
  // Multiple predicates combine with And.
  auto combined = translate_filter(
    ir::OptimizeFilter{parse_tql("id > 10"), parse_tql("name == \"alice\"")},
    schema);
  auto expected = ice::Expressions::And(
    ice::Expressions::GreaterThan("id", ice::Literal::Long(10)),
    ice::Expressions::Equal("name", ice::Literal::String("alice")));
  CHECK(combined->Equals(*expected));
  // An AlwaysFalse predicate short-circuits the conjunction.
  auto pruned = translate_filter(
    ir::OptimizeFilter{parse_tql("id > 10"), parse_tql("missing? != null"),
                       parse_tql("name == \"alice\"")},
    schema);
  CHECK(pruned->Equals(*ice::Expressions::AlwaysFalse()));
  // Conservative predicates fold cleanly out of And.
  auto partial = translate_filter(
    ir::OptimizeFilter{parse_tql("id == \"mismatch\""), parse_tql("id > 10")},
    schema);
  CHECK(partial->Equals(
    *ice::Expressions::GreaterThan("id", ice::Literal::Long(10))));
}

TEST("comparison operators, flipped operands, and bare predicates") {
  auto schema = make_test_schema();
  // All six comparison operators.
  CHECK(translate("id == 42", schema)
          ->Equals(*ice::Expressions::Equal("id", ice::Literal::Long(42))));
  CHECK(translate("id != 42", schema)
          ->Equals(*ice::Expressions::NotEqual("id", ice::Literal::Long(42))));
  CHECK(translate("id < 42", schema)
          ->Equals(*ice::Expressions::LessThan("id", ice::Literal::Long(42))));
  CHECK(translate("id <= 42", schema)
          ->Equals(
            *ice::Expressions::LessThanOrEqual("id", ice::Literal::Long(42))));
  CHECK(
    translate("id > 42", schema)
      ->Equals(*ice::Expressions::GreaterThan("id", ice::Literal::Long(42))));
  CHECK(translate("id >= 42", schema)
          ->Equals(*ice::Expressions::GreaterThanOrEqual(
            "id", ice::Literal::Long(42))));
  // Flipped operands reverse ordering operators.
  CHECK(translate("42 == id", schema)
          ->Equals(*ice::Expressions::Equal("id", ice::Literal::Long(42))));
  CHECK(translate("42 != id", schema)
          ->Equals(*ice::Expressions::NotEqual("id", ice::Literal::Long(42))));
  CHECK(
    translate("42 < id", schema)
      ->Equals(*ice::Expressions::GreaterThan("id", ice::Literal::Long(42))));
  CHECK(translate("42 <= id", schema)
          ->Equals(*ice::Expressions::GreaterThanOrEqual(
            "id", ice::Literal::Long(42))));
  CHECK(translate("42 > id", schema)
          ->Equals(*ice::Expressions::LessThan("id", ice::Literal::Long(42))));
  CHECK(translate("42 >= id", schema)
          ->Equals(
            *ice::Expressions::LessThanOrEqual("id", ice::Literal::Long(42))));
  // Negative and unary-plus constants.
  CHECK(translate("i32 == -42", schema)
          ->Equals(*ice::Expressions::Equal("i32", ice::Literal::Int(-42))));
  {
    auto pos_expr = ast::expression{ast::binary_expr{
      ast::expression{
        ast::root_field{ast::identifier{"i32", location::unknown}}},
      ast::binary_op::eq,
      ast::expression{ast::unary_expr{
        located{ast::unary_op::pos, location::unknown},
        ast::expression{ast::constant{int64_t{42}, location::unknown}},
      }},
    }};
    CHECK(translate_expression(pos_expr, schema)
            ->Equals(*ice::Expressions::Equal("i32", ice::Literal::Int(42))));
  }
  CHECK(translate("id == -9223372036854775808", schema)
          ->Equals(*ice::Expressions::Equal(
            "id", ice::Literal::Long(std::numeric_limits<int64_t>::min()))));
  // Boolean and string comparisons.
  CHECK(
    translate("flag == true", schema)
      ->Equals(*ice::Expressions::Equal("flag", ice::Literal::Boolean(true))));
  CHECK(translate("flag < true", schema)
          ->Equals(
            *ice::Expressions::LessThan("flag", ice::Literal::Boolean(true))));
  CHECK(translate("name >= \"abc\"", schema)
          ->Equals(*ice::Expressions::GreaterThanOrEqual(
            "name", ice::Literal::String("abc"))));
  // Bare boolean fields and constants.
  CHECK(
    translate("flag", schema)
      ->Equals(*ice::Expressions::Equal("flag", ice::Literal::Boolean(true))));
  CHECK(translate("not flag", schema)
          ->Equals(
            *ice::Expressions::NotEqual("flag", ice::Literal::Boolean(true))));
  CHECK(translate("id", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("true", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("false", schema)->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("null", schema)->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("42", schema)->Equals(*ice::Expressions::AlwaysTrue()));
}

TEST("null comparisons and their negations") {
  auto schema = make_test_schema();
  CHECK(
    translate("id == null", schema)->Equals(*ice::Expressions::IsNull("id")));
  CHECK(
    translate("null == id", schema)->Equals(*ice::Expressions::IsNull("id")));
  CHECK(
    translate("id != null", schema)->Equals(*ice::Expressions::NotNull("id")));
  CHECK(
    translate("null != id", schema)->Equals(*ice::Expressions::NotNull("id")));
  // In TQL, `null <= null` and `null >= null` are true, while `null < null`
  // and `null > null` are null.
  CHECK(
    translate("id <= null", schema)->Equals(*ice::Expressions::IsNull("id")));
  CHECK(
    translate("id >= null", schema)->Equals(*ice::Expressions::IsNull("id")));
  CHECK(
    translate("id < null", schema)->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(
    translate("id > null", schema)->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("not (id == null)", schema)
          ->Equals(*ice::Expressions::NotNull("id")));
  CHECK(translate("not (id != null)", schema)
          ->Equals(*ice::Expressions::IsNull("id")));
  // `date`, `timestamp`, and `timestamptz` overflow out-of-range values
  // (outside ~[1677, 2262]) to `null` at read time, so `IsNull` pruning would
  // falsely drop non-null files containing e.g. `9999-12-31`.
  CHECK(
    translate("day == null", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("day <= null", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("day >= null", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("ts == null", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("ts_tz == null", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("day in [null]", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("day in [2024-01-15, null]", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("ts in [2024-01-15T00:00:00Z, null]", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // `!= null` stays `NotNull` (sound on all-null files), but `exact = false`
  // prevents `not (col != null)` from negating back to `IsNull`.
  CHECK(
    translate("day != null", schema)->Equals(*ice::Expressions::NotNull("day")));
  CHECK(
    translate("ts != null", schema)->Equals(*ice::Expressions::NotNull("ts")));
  CHECK(translate("ts_tz != null", schema)
          ->Equals(*ice::Expressions::NotNull("ts_tz")));
  CHECK(translate("not (day != null)", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("not (ts != null)", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("not (ts_tz != null)", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // Nanosecond timestamp columns never overflow at read time, so `IsNull`
  // remains exact.
  CHECK(translate("ts_ns == null", schema)
          ->Equals(*ice::Expressions::IsNull("ts_ns")));
  CHECK(translate("ts_tz_ns == null", schema)
          ->Equals(*ice::Expressions::IsNull("ts_tz_ns")));
  CHECK(translate("not (ts_ns != null)", schema)
          ->Equals(*ice::Expressions::IsNull("ts_ns")));
}

TEST("int32, int64, and uint64 overflow boundaries") {
  auto schema = make_test_schema();
  // Exact int32 endpoints stay literal comparisons.
  CHECK(translate("i32 == 2147483647", schema)
          ->Equals(*ice::Expressions::Equal(
            "i32", ice::Literal::Int(std::numeric_limits<int32_t>::max()))));
  CHECK(translate("i32 == -2147483648", schema)
          ->Equals(*ice::Expressions::Equal(
            "i32", ice::Literal::Int(std::numeric_limits<int32_t>::min()))));
  // Above INT32_MAX.
  CHECK(translate("i32 == 5000000000", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("i32 > 5000000000", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("i32 >= 5000000000", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("i32 < 5000000000", schema)
          ->Equals(*ice::Expressions::NotNull("i32")));
  CHECK(translate("i32 <= 5000000000", schema)
          ->Equals(*ice::Expressions::NotNull("i32")));
  // `null != 5000000000` is true in TQL, so `!=` must stay AlwaysTrue.
  CHECK(translate("i32 != 5000000000", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("not (i32 != 5000000000)", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  // Below INT32_MIN.
  CHECK(translate("i32 == -5000000000", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("i32 < -5000000000", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("i32 <= -5000000000", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("i32 > -5000000000", schema)
          ->Equals(*ice::Expressions::NotNull("i32")));
  CHECK(translate("i32 >= -5000000000", schema)
          ->Equals(*ice::Expressions::NotNull("i32")));
  CHECK(translate("i32 != -5000000000", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // uint64 values exceeding INT64_MAX against int64 column.
  CHECK(translate("id == 9223372036854775808", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("id == 18446744073709551615", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("id > 9223372036854775808", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("id < 9223372036854775808", schema)
          ->Equals(*ice::Expressions::NotNull("id")));
  CHECK(translate("id != 9223372036854775808", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // Verify on an all-null file that `i32 != 5000000000` keeps the file while
  // `i32 < 5000000000` prunes it.
  auto all_null = make_all_null_file(i32_col);
  CHECK(eval_metrics(translate("i32 != 5000000000", schema), schema, all_null));
  CHECK(
    not eval_metrics(translate("i32 < 5000000000", schema), schema, all_null));
}

TEST("floating point precision, NaN, signed zero, and negation conservatism") {
  auto schema = make_test_schema();
  // Exact float64 and float32 comparisons.
  CHECK(translate("f64 == 1.5", schema)
          ->Equals(*ice::Expressions::Equal("f64", ice::Literal::Double(1.5))));
  CHECK(translate("f64 < -2.25", schema)
          ->Equals(
            *ice::Expressions::LessThan("f64", ice::Literal::Double(-2.25))));
  CHECK(translate("f32 == 0.5", schema)
          ->Equals(*ice::Expressions::Equal("f32", ice::Literal::Float(0.5f))));
  // 0.1 cannot be represented losslessly as float32 -> conservative.
  CHECK(
    translate("f32 == 0.1", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  // Integer constants exact in IEEE-754 push down to float/double columns.
  CHECK(translate("f64 > 10", schema)
          ->Equals(
            *ice::Expressions::GreaterThan("f64", ice::Literal::Double(10))));
  // `!=` and `not(...)` on float/double stay conservative because of NaN.
  CHECK(
    translate("f64 != 1.5", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("not (f64 == 1.5)", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("not (f64 < 1.5)", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("not (f32 == 0.5)", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // Direct AST with NaN and infinity stays conservative.
  auto make_double_cmp = [](ast::binary_op op, double v) -> ast::expression {
    return ast::binary_expr{
      ast::root_field{ast::identifier{"f64", location::unknown}, false},
      op,
      ast::constant{v, location::unknown},
    };
  };
  CHECK(translate_expression(
          make_double_cmp(ast::binary_op::eq,
                          std::numeric_limits<double>::quiet_NaN()),
          schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate_expression(
          make_double_cmp(ast::binary_op::lt,
                          std::numeric_limits<double>::quiet_NaN()),
          schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate_expression(
          make_double_cmp(ast::binary_op::gt,
                          std::numeric_limits<double>::infinity()),
          schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // Signed zero: Iceberg orders -0.0 < +0.0, while TQL equates -0.0 == +0.0.
  auto neg_zero_file = make_data_file(f64_col, ice::Literal::Double(-0.0),
                                      ice::Literal::Double(-0.0));
  auto pos_zero_file = make_data_file(f64_col, ice::Literal::Double(0.0),
                                      ice::Literal::Double(0.0));
  CHECK(eval_metrics(translate("f64 == 0.0", schema), schema, neg_zero_file));
  CHECK(eval_metrics(translate("f64 == -0.0", schema), schema, pos_zero_file));
  CHECK(eval_metrics(translate("f64 >= 0.0", schema), schema, neg_zero_file));
  CHECK(eval_metrics(translate("f64 <= -0.0", schema), schema, pos_zero_file));
  CHECK(
    not eval_metrics(translate("f64 < 0.0", schema), schema, neg_zero_file));
  CHECK(
    not eval_metrics(translate("f64 > -0.0", schema), schema, pos_zero_file));
}

TEST("nested struct paths, map/list/variant ancestors, and missing fields") {
  auto schema = make_test_schema();
  // Struct paths resolve to dotted Iceberg names.
  CHECK(
    translate("nested.a == 7", schema)
      ->Equals(*ice::Expressions::Equal("nested.a", ice::Literal::Long(7))));
  CHECK(translate("nested.inner.x > 0", schema)
          ->Equals(*ice::Expressions::GreaterThan("nested.inner.x",
                                                  ice::Literal::Int(0))));
  CHECK(translate("this.nested.b == \"hi\"", schema)
          ->Equals(
            *ice::Expressions::Equal("nested.b", ice::Literal::String("hi"))));
  // Non-struct ancestors must stay conservative (map imports as dynamic record).
  CHECK(translate("labels.env == \"prod\"", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("labels.env != null", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("items.k == 1", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("payload.foo == 1", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // Non-primitive leaf columns stay conservative (Iceberg cannot bind them).
  CHECK(translate("nested == null", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("labels != null", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // Missing fields WITHOUT `?` emit a warning in TQL, so they must stay
  // conservative to avoid silently suppressing diagnostics.
  CHECK(translate("missing == null", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("missing != null", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("missing == 5", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("nested.missing != null", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("missing?.sub != null", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // Missing fields WITH `?` suppress warnings and behave as null.
  CHECK(translate("missing? == null", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("missing? != null", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("missing? <= null", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("missing? < null", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("missing? == 5", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  // In TQL, `null != 5` is true.
  CHECK(
    translate("missing? != 5", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("missing? < 5", schema)->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("nested.missing? != null", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("missing?.sub? != null", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("not (missing? != null)", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("not (missing? == null)", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
}

TEST("boolean connectives and negation exactness propagation") {
  auto schema = make_test_schema();
  // Exact conjunction and disjunction.
  auto conj = translate("id > 10 and name == \"a\"", schema);
  CHECK(conj->Equals(*ice::Expressions::And(
    ice::Expressions::GreaterThan("id", ice::Literal::Long(10)),
    ice::Expressions::Equal("name", ice::Literal::String("a")))));
  auto disj = translate("id > 10 or name == \"a\"", schema);
  CHECK(disj->Equals(*ice::Expressions::Or(
    ice::Expressions::GreaterThan("id", ice::Literal::Long(10)),
    ice::Expressions::Equal("name", ice::Literal::String("a")))));
  // De Morgan negation of exact expressions.
  CHECK(translate("not (id == 1 and name == \"a\")", schema)
          ->Equals(*ice::Expressions::Or(
            ice::Expressions::NotEqual("id", ice::Literal::Long(1)),
            ice::Expressions::NotEqual("name", ice::Literal::String("a")))));
  CHECK(translate("not (id == 1 or name == \"a\")", schema)
          ->Equals(*ice::Expressions::And(
            ice::Expressions::NotEqual("id", ice::Literal::Long(1)),
            ice::Expressions::NotEqual("name", ice::Literal::String("a")))));
  CHECK(translate("not (id < 10)", schema)
          ->Equals(*ice::Expressions::GreaterThanOrEqual(
            "id", ice::Literal::Long(10))));
  CHECK(translate("not (not (id == 1))", schema)
          ->Equals(*ice::Expressions::Equal("id", ice::Literal::Long(1))));
  // Over-approximation under `and` folds to the exact side, but MUST NOT be
  // negated under `not(...)`.
  CHECK(
    translate("id > 10 and id == \"bad\"", schema)
      ->Equals(*ice::Expressions::GreaterThan("id", ice::Literal::Long(10))));
  CHECK(translate("not (id > 10 and id == \"bad\")", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // Over-approximation under `or` collapses to AlwaysTrue.
  CHECK(translate("id > 10 or id == \"bad\"", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
}

TEST("in and not in predicates") {
  auto schema = make_test_schema();
  CHECK(translate("id in [1, 2, 3]", schema)
          ->Equals(*ice::Expressions::In("id", {ice::Literal::Long(1),
                                                ice::Literal::Long(2),
                                                ice::Literal::Long(3)})));
  CHECK(translate("id not in [1, 2, 3]", schema)
          ->Equals(*ice::Expressions::NotIn("id", {ice::Literal::Long(1),
                                                   ice::Literal::Long(2),
                                                   ice::Literal::Long(3)})));
  // Empty list.
  CHECK(
    translate("id in []", schema)->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(
    translate("id not in []", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  // Null inside list: `null in [1, null]` is true in TQL!
  CHECK(translate("id in [1, null]", schema)
          ->Equals(*ice::Expressions::Or(
            ice::Expressions::IsNull("id"),
            ice::Expressions::In("id", {ice::Literal::Long(1)}))));
  CHECK(translate("id not in [1, null]", schema)
          ->Equals(*ice::Expressions::And(
            ice::Expressions::NotNull("id"),
            ice::Expressions::NotIn("id", {ice::Literal::Long(1)}))));
  CHECK(
    translate("id in [null]", schema)->Equals(*ice::Expressions::IsNull("id")));
  CHECK(translate("id not in [null]", schema)
          ->Equals(*ice::Expressions::NotNull("id")));
  // Out-of-range integer elements drop out of the literal set.
  CHECK(translate("i32 in [1, 5000000000]", schema)
          ->Equals(*ice::Expressions::In("i32", {ice::Literal::Int(1)})));
  CHECK(translate("i32 in [5000000000]", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  // Incompatible or non-constant elements stay conservative.
  CHECK(translate("id in [1, \"bad\"]", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("id in [1, i32]", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("f64 not in [1.0, 2.0]", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // Missing columns: silent `?` folds, non-`?` stays conservative.
  CHECK(translate("missing in [1, 2]", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("missing? in [1, 2]", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("missing? in [1, null]", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("missing? not in [1, 2]", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
}

TEST("starts_with method and function syntax") {
  auto schema = make_test_schema();
  CHECK(translate("name.starts_with(\"abc\")", schema)
          ->Equals(*ice::Expressions::StartsWith("name", "abc")));
  CHECK(translate("starts_with(name, \"abc\")", schema)
          ->Equals(*ice::Expressions::StartsWith("name", "abc")));
  CHECK(translate("not name.starts_with(\"abc\")", schema)
          ->Equals(*ice::Expressions::NotStartsWith("name", "abc")));
  // Named options (e.g. ignore_case) stay conservative.
  CHECK(translate("name.starts_with(\"abc\", ignore_case=true)", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("name.starts_with(\"abc\", ignore_case=false)", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  // Non-string column and non-`?` missing column stay conservative; silent
  // missing column (`?`) folds to AlwaysFalse.
  CHECK(translate("id.starts_with(\"1\")", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("missing.starts_with(\"abc\")", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("missing?.starts_with(\"abc\")", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
}

TEST("timestamps, negative pre-1970 sub-microsecond, dates, and time") {
  auto schema = make_test_schema();
  // Exact microsecond timestamp.
  auto const t_us = epoch_us(2024, 1, 15, 12, 30, 0, 1);
  CHECK(
    translate("ts == 2024-01-15T12:30:00.000001Z", schema)
      ->Equals(*ice::Expressions::Equal("ts", ice::Literal::Timestamp(t_us))));
  CHECK(translate("ts_tz == 2024-01-15T12:30:00.000001Z", schema)
          ->Equals(*ice::Expressions::Equal("ts_tz",
                                            ice::Literal::TimestampTz(t_us))));
  // Positive sub-microsecond timestamp (`t_us` + 500ns).
  CHECK(translate("ts == 2024-01-15T12:30:00.000001500Z", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("ts != 2024-01-15T12:30:00.000001500Z", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("ts > 2024-01-15T12:30:00.000001500Z", schema)
          ->Equals(*ice::Expressions::GreaterThan(
            "ts", ice::Literal::Timestamp(t_us))));
  CHECK(translate("ts >= 2024-01-15T12:30:00.000001500Z", schema)
          ->Equals(*ice::Expressions::GreaterThan(
            "ts", ice::Literal::Timestamp(t_us))));
  CHECK(translate("ts < 2024-01-15T12:30:00.000001500Z", schema)
          ->Equals(*ice::Expressions::LessThanOrEqual(
            "ts", ice::Literal::Timestamp(t_us))));
  CHECK(translate("ts <= 2024-01-15T12:30:00.000001500Z", schema)
          ->Equals(*ice::Expressions::LessThanOrEqual(
            "ts", ice::Literal::Timestamp(t_us))));
  // Negative pre-1970 exact microsecond (`-1 us`).
  CHECK(
    translate("ts == 1969-12-31T23:59:59.999999Z", schema)
      ->Equals(*ice::Expressions::Equal("ts", ice::Literal::Timestamp(-1))));
  // Negative pre-1970 sub-microsecond (`-500 ns`, Euclidean floor is `-1 us`,
  // remainder `+500 ns`, NOT C++ truncated `0 us`!).
  CHECK(translate("ts == 1969-12-31T23:59:59.999999500Z", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("ts != 1969-12-31T23:59:59.999999500Z", schema)
          ->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(translate("ts > 1969-12-31T23:59:59.999999500Z", schema)
          ->Equals(
            *ice::Expressions::GreaterThan("ts", ice::Literal::Timestamp(-1))));
  CHECK(translate("ts >= 1969-12-31T23:59:59.999999500Z", schema)
          ->Equals(
            *ice::Expressions::GreaterThan("ts", ice::Literal::Timestamp(-1))));
  CHECK(translate("ts < 1969-12-31T23:59:59.999999500Z", schema)
          ->Equals(*ice::Expressions::LessThanOrEqual(
            "ts", ice::Literal::Timestamp(-1))));
  CHECK(translate("ts <= 1969-12-31T23:59:59.999999500Z", schema)
          ->Equals(*ice::Expressions::LessThanOrEqual(
            "ts", ice::Literal::Timestamp(-1))));
  // Nanosecond timestamp columns preserve full nanosecond precision.
  CHECK(translate("ts_ns == 1969-12-31T23:59:59.999999500Z", schema)
          ->Equals(*ice::Expressions::Equal("ts_ns",
                                            ice::Literal::TimestampNs(-500))));
  CHECK(translate("ts_tz_ns == 1969-12-31T23:59:59.999999500Z", schema)
          ->Equals(*ice::Expressions::Equal(
            "ts_tz_ns", ice::Literal::TimestampTzNs(-500))));
  // Date column rounding (both post-1970 and pre-1970).
  auto const jan15 = epoch_days(2024, 1, 15);
  CHECK(translate("day == 2024-01-15", schema)
          ->Equals(*ice::Expressions::Equal("day", ice::Literal::Date(jan15))));
  CHECK(translate("day == 2024-01-15T12:00:00Z", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(translate("day > 2024-01-15T12:00:00Z", schema)
          ->Equals(
            *ice::Expressions::GreaterThan("day", ice::Literal::Date(jan15))));
  CHECK(translate("day >= 2024-01-15T12:00:00Z", schema)
          ->Equals(
            *ice::Expressions::GreaterThan("day", ice::Literal::Date(jan15))));
  CHECK(translate("day <= 2024-01-15T12:00:00Z", schema)
          ->Equals(*ice::Expressions::LessThanOrEqual(
            "day", ice::Literal::Date(jan15))));
  CHECK(translate("day < 1969-12-31T12:00:00Z", schema)
          ->Equals(
            *ice::Expressions::LessThanOrEqual("day", ice::Literal::Date(-1))));
  CHECK(
    translate("day >= 1969-12-31T12:00:00Z", schema)
      ->Equals(*ice::Expressions::GreaterThan("day", ice::Literal::Date(-1))));
  // Time-of-day column (`duration` in [0, 24h)).
  CHECK(translate("tod == 1h", schema)
          ->Equals(*ice::Expressions::Equal(
            "tod", ice::Literal::Time(3'600'000'000))));
  CHECK(translate("tod == 1234ns", schema)
          ->Equals(*ice::Expressions::AlwaysFalse()));
  CHECK(
    translate("tod > 1234ns", schema)
      ->Equals(*ice::Expressions::GreaterThan("tod", ice::Literal::Time(1))));
  CHECK(translate("tod <= 1234ns", schema)
          ->Equals(
            *ice::Expressions::LessThanOrEqual("tod", ice::Literal::Time(1))));
  CHECK(
    translate("tod == -1s", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("tod == 25h", schema)->Equals(*ice::Expressions::AlwaysTrue()));
}

TEST("type mismatches and unsupported expressions remain conservative") {
  auto schema = make_test_schema();
  CHECK(
    translate("name == 42", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("id == \"42\"", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("flag == 1", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("ts == 12345", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("dec == 1.5", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("id == i32", schema)->Equals(*ice::Expressions::AlwaysTrue()));
  CHECK(
    translate("id + 1 == 5", schema)->Equals(*ice::Expressions::AlwaysTrue()));
}

TEST("InclusiveMetricsEvaluator prunes and preserves boundary files") {
  auto schema = make_test_schema();
  // Integer range [10, 20].
  auto id_file
    = make_data_file(id_col, ice::Literal::Long(10), ice::Literal::Long(20));
  CHECK(not eval_metrics(translate("id == 5", schema), schema, id_file));
  CHECK(eval_metrics(translate("id == 15", schema), schema, id_file));
  CHECK(not eval_metrics(translate("id > 20", schema), schema, id_file));
  CHECK(eval_metrics(translate("id >= 20", schema), schema, id_file));
  // All-null file: verify TQL null semantics are never violated.
  auto null_id_file = make_all_null_file(id_col);
  CHECK(eval_metrics(translate("id == null", schema), schema, null_id_file));
  CHECK(
    not eval_metrics(translate("id != null", schema), schema, null_id_file));
  CHECK(eval_metrics(translate("id != 15", schema), schema, null_id_file));
  CHECK(
    eval_metrics(translate("not (id == 15)", schema), schema, null_id_file));
  CHECK(
    not eval_metrics(translate("not (id < 15)", schema), schema, null_id_file));
  CHECK(
    eval_metrics(translate("id in [15, null]", schema), schema, null_id_file));
  CHECK(not eval_metrics(translate("id not in [15, null]", schema), schema,
                         null_id_file));
  CHECK(
    eval_metrics(translate("id not in [15]", schema), schema, null_id_file));
  // Pre-1970 sub-microsecond boundary (-500 ns sits between -1 us and 0 us).
  auto file_neg1_us = make_data_file(ts_col, ice::Literal::Timestamp(-1),
                                     ice::Literal::Timestamp(-1));
  auto file_zero_us = make_data_file(ts_col, ice::Literal::Timestamp(0),
                                     ice::Literal::Timestamp(0));
  CHECK(
    not eval_metrics(translate("ts > 1969-12-31T23:59:59.999999500Z", schema),
                     schema, file_neg1_us));
  CHECK(eval_metrics(translate("ts < 1969-12-31T23:59:59.999999500Z", schema),
                     schema, file_neg1_us));
  CHECK(eval_metrics(translate("ts > 1969-12-31T23:59:59.999999500Z", schema),
                     schema, file_zero_us));
  CHECK(
    not eval_metrics(translate("ts < 1969-12-31T23:59:59.999999500Z", schema),
                     schema, file_zero_us));
  // Sub-day boundary on date column.
  auto const jan15 = epoch_days(2024, 1, 15);
  auto day_file = make_data_file(day_col, ice::Literal::Date(jan15),
                                 ice::Literal::Date(jan15));
  CHECK(
    not eval_metrics(translate("day > 2024-01-15T00:00:00.000000001Z", schema),
                     schema, day_file));
  CHECK(eval_metrics(translate("day < 2024-01-15T00:00:00.000000001Z", schema),
                     schema, day_file));
  CHECK(eval_metrics(translate("day >= 2024-01-15", schema), schema, day_file));
  // Out-of-64-bit-nanosecond-range date (`9999-12-31`) becomes `null` at read
  // time: `== null`, `not (!= null)`, and `in [..., null]` must NOT prune this
  // non-null file, whereas `!= null` still prunes an all-null file.
  auto const far_future_day = epoch_days(9999, 12, 31);
  auto far_future_file
    = make_data_file(day_col, ice::Literal::Date(far_future_day),
                     ice::Literal::Date(far_future_day));
  CHECK(
    eval_metrics(translate("day == null", schema), schema, far_future_file));
  CHECK(eval_metrics(translate("not (day != null)", schema), schema,
                     far_future_file));
  CHECK(eval_metrics(translate("day in [2024-01-15, null]", schema), schema,
                     far_future_file));
  auto all_null_day = make_all_null_file(day_col);
  CHECK(
    not eval_metrics(translate("day != null", schema), schema, all_null_day));
  auto non_null_ts_ns = make_data_file(ts_ns_col, ice::Literal::TimestampNs(0),
                                       ice::Literal::TimestampNs(1'000'000));
  CHECK(not eval_metrics(translate("ts_ns == null", schema), schema,
                         non_null_ts_ns));
  // String prefix pruning.
  auto name_file = make_data_file(name_col, ice::Literal::String("alpha"),
                                  ice::Literal::String("apricot"));
  CHECK(eval_metrics(translate("name.starts_with(\"ap\")", schema), schema,
                     name_file));
  CHECK(not eval_metrics(translate("name.starts_with(\"ba\")", schema), schema,
                         name_file));
  CHECK(not eval_metrics(translate("not name.starts_with(\"a\")", schema),
                         schema, name_file));
  // Float NaN safety: even with nan_value_counts omitted, `!=` and `not(...)`
  // must not prune.
  auto f64_const_file = make_data_file(f64_col, ice::Literal::Double(5.0),
                                       ice::Literal::Double(5.0));
  CHECK(eval_metrics(translate("f64 != 5.0", schema), schema, f64_const_file));
  CHECK(eval_metrics(translate("not (f64 == 5.0)", schema), schema,
                     f64_const_file));
  auto all_nan_file = ice::DataFile{};
  all_nan_file.record_count = 10;
  all_nan_file.value_counts[f64_col] = 10;
  all_nan_file.null_value_counts[f64_col] = 0;
  all_nan_file.nan_value_counts[f64_col] = 10;
  CHECK(
    eval_metrics(translate("not (f64 < 5.0)", schema), schema, all_nan_file));
  // Nested struct column pruning.
  auto nested_file = make_data_file(nested_a_col, ice::Literal::Long(100),
                                    ice::Literal::Long(200));
  CHECK(
    not eval_metrics(translate("nested.a < 50", schema), schema, nested_file));
  CHECK(
    eval_metrics(translate("nested.a == 150", schema), schema, nested_file));
}

TEST("ManifestEvaluator prunes manifests using partition summaries") {
  auto schema = make_test_schema();
  auto spec_res = ice::PartitionSpec::Make(
    schema, 0,
    {
      ice::PartitionField{name_col, 1000, "name_part",
                          ice::Transform::Identity()},
      ice::PartitionField{ts_tz_col, 1001, "ts_day", ice::Transform::Day()},
    },
    /*allow_missing_fields=*/false);
  REQUIRE(spec_res.has_value());
  auto spec = std::shared_ptr<ice::PartitionSpec>{*std::move(spec_res)};
  // Manifest covers name in ["a", "c"] and day(ts_tz) in [2024-01-01,
  // 2024-01-10].
  auto manifest = ice::ManifestFile{};
  auto name_summary = ice::PartitionFieldSummary{};
  name_summary.contains_null = false;
  name_summary.lower_bound = serialize(ice::Literal::String("a"));
  name_summary.upper_bound = serialize(ice::Literal::String("c"));
  auto day_summary = ice::PartitionFieldSummary{};
  day_summary.contains_null = false;
  day_summary.lower_bound
    = serialize(ice::Literal::Date(epoch_days(2024, 1, 1)));
  day_summary.upper_bound
    = serialize(ice::Literal::Date(epoch_days(2024, 1, 10)));
  manifest.partitions = {std::move(name_summary), std::move(day_summary)};
  // Identity string partition pruning.
  CHECK(not eval_manifest(translate("name == \"d\"", schema), spec, schema,
                          manifest));
  CHECK(
    eval_manifest(translate("name == \"b\"", schema), spec, schema, manifest));
  CHECK(not eval_manifest(translate("name.starts_with(\"z\")", schema), spec,
                          schema, manifest));
  CHECK(eval_manifest(translate("name.starts_with(\"b\")", schema), spec,
                      schema, manifest));
  // Day-transformed timestamp partition pruning, including sub-microsecond
  // boundaries at the end of 2024-01-10.
  CHECK(not eval_manifest(translate("ts_tz >= 2024-01-11T00:00:00Z", schema),
                          spec, schema, manifest));
  // 2024-01-10T23:59:59.999999500Z exceeds the last microsecond of 2024-01-10,
  // so no microsecond row in this manifest can match.
  CHECK(not eval_manifest(translate("ts_tz >= 2024-01-10T23:59:59.999999500Z",
                                    schema),
                          spec, schema, manifest));
  // One microsecond earlier, 2024-01-10T23:59:59.999999Z still matches.
  CHECK(
    eval_manifest(translate("ts_tz >= 2024-01-10T23:59:59.999998500Z", schema),
                  spec, schema, manifest));
  CHECK(not eval_manifest(translate("ts_tz < 2024-01-01T00:00:00Z", schema),
                          spec, schema, manifest));
  // Silent missing-field AlwaysFalse prunes; non-`?` missing field and
  // map-ancestor AlwaysTrue keep.
  CHECK(not eval_manifest(translate("missing? != null", schema), spec, schema,
                          manifest));
  CHECK(eval_manifest(translate("missing != null", schema), spec, schema,
                      manifest));
  CHECK(eval_manifest(translate("labels.env == \"prod\"", schema), spec, schema,
                      manifest));
}

TEST("ManifestEvaluator keeps manifests with an inconsistent summary") {
  auto schema = make_test_schema();
  auto spec_res = ice::PartitionSpec::Make(
    schema, 0,
    {
      ice::PartitionField{name_col, 1000, "name_part",
                          ice::Transform::Identity()},
    },
    /*allow_missing_fields=*/false);
  REQUIRE(spec_res.has_value());
  auto spec = std::shared_ptr<ice::PartitionSpec>{*std::move(spec_res)};
  auto manifest_with = [](ice::PartitionFieldSummary summary) {
    auto manifest = ice::ManifestFile{};
    manifest.partitions = {std::move(summary)};
    return manifest;
  };
  auto keeps = [&](std::string_view predicate,
                   ice::ManifestFile const& manifest) {
    return eval_manifest(translate(predicate, schema), spec, schema, manifest);
  };
  // A writer dropped the lower bound, the empty string, of a manifest that
  // holds null, "", "..", and "x": an upper bound proves that values exist, so
  // the missing lower bound is unbounded.
  auto no_lower = ice::PartitionFieldSummary{};
  no_lower.contains_null = true;
  no_lower.contains_nan = false;
  no_lower.upper_bound = serialize(ice::Literal::String("x"));
  auto const no_lower_manifest = manifest_with(std::move(no_lower));
  CHECK(keeps("name == \"..\"", no_lower_manifest));
  CHECK(keeps("name == \"\"", no_lower_manifest));
  CHECK(keeps("name < \"b\"", no_lower_manifest));
  CHECK(keeps("name <= \"b\"", no_lower_manifest));
  CHECK(keeps("name in [\"..\", \"a\"]", no_lower_manifest));
  CHECK(keeps("name.starts_with(\".\")", no_lower_manifest));
  CHECK(keeps("name != null", no_lower_manifest));
  CHECK(keeps("name == null", no_lower_manifest));
  // The bound that is present still prunes.
  CHECK(not keeps("name == \"y\"", no_lower_manifest));
  CHECK(not keeps("name > \"x\"", no_lower_manifest));
  CHECK(not keeps("name >= \"y\"", no_lower_manifest));
  // The mirrored case: only the lower bound is present.
  auto no_upper = ice::PartitionFieldSummary{};
  no_upper.contains_null = false;
  no_upper.contains_nan = false;
  no_upper.lower_bound = serialize(ice::Literal::String("b"));
  auto const no_upper_manifest = manifest_with(std::move(no_upper));
  CHECK(keeps("name == \"c\"", no_upper_manifest));
  CHECK(keeps("name > \"c\"", no_upper_manifest));
  CHECK(keeps("name >= \"c\"", no_upper_manifest));
  CHECK(not keeps("name == \"a\"", no_upper_manifest));
  CHECK(not keeps("name < \"b\"", no_upper_manifest));
  // No bound, but neither null nor NaN values: the only value was the empty
  // string, and the bounds are unknown.
  auto no_bounds = ice::PartitionFieldSummary{};
  no_bounds.contains_null = false;
  no_bounds.contains_nan = false;
  auto const no_bounds_manifest = manifest_with(std::move(no_bounds));
  CHECK(keeps("name == \"\"", no_bounds_manifest));
  CHECK(keeps("name <= \"b\"", no_bounds_manifest));
  CHECK(keeps("name >= \"b\"", no_bounds_manifest));
  // No bound and null values. For a string, this is also the summary that a
  // writer left for a manifest with both null and "" after it dropped the two
  // empty bounds, so it proves nothing about the values that are not null.
  auto null_and_empty = ice::PartitionFieldSummary{};
  null_and_empty.contains_null = true;
  null_and_empty.contains_nan = false;
  auto const null_and_empty_manifest = manifest_with(std::move(null_and_empty));
  CHECK(keeps("name == \"\"", null_and_empty_manifest));
  CHECK(keeps("name != null", null_and_empty_manifest));
  CHECK(keeps("name == \"x\"", null_and_empty_manifest));
  CHECK(keeps("name < \"b\"", null_and_empty_manifest));
  CHECK(keeps("name <= \"b\"", null_and_empty_manifest));
  CHECK(keeps("name > \"b\"", null_and_empty_manifest));
  CHECK(keeps("name >= \"b\"", null_and_empty_manifest));
  CHECK(keeps("name in [\"..\", \"a\"]", null_and_empty_manifest));
  CHECK(keeps("name.starts_with(\".\")", null_and_empty_manifest));
  CHECK(keeps("name == null", null_and_empty_manifest));
  // The count of null values is not affected: without any, `== null` prunes.
  CHECK(not keeps("name == null", no_bounds_manifest));
}

TEST("ManifestEvaluator prunes on an all-null summary of a type without empty "
     "bounds") {
  auto schema = make_test_schema();
  auto spec_res = ice::PartitionSpec::Make(
    schema, 0,
    {
      ice::PartitionField{id_col, 1000, "id_part", ice::Transform::Identity()},
    },
    /*allow_missing_fields=*/false);
  REQUIRE(spec_res.has_value());
  auto spec = std::shared_ptr<ice::PartitionSpec>{*std::move(spec_res)};
  auto keeps = [&](std::string_view predicate,
                   ice::ManifestFile const& manifest) {
    return eval_manifest(translate(predicate, schema), spec, schema, manifest);
  };
  // The bound of an integer never serializes to zero bytes, so no writer
  // dropped it: no bound and null values mean that every value is null.
  auto all_null = ice::PartitionFieldSummary{};
  all_null.contains_null = true;
  all_null.contains_nan = false;
  auto all_null_manifest = ice::ManifestFile{};
  all_null_manifest.partitions = {std::move(all_null)};
  CHECK(not keeps("id == 5", all_null_manifest));
  CHECK(not keeps("id < 5", all_null_manifest));
  CHECK(not keeps("id <= 5", all_null_manifest));
  CHECK(not keeps("id > 5", all_null_manifest));
  CHECK(not keeps("id >= 5", all_null_manifest));
  CHECK(not keeps("id in [1, 2]", all_null_manifest));
  CHECK(not keeps("id != null", all_null_manifest));
  CHECK(keeps("id == null", all_null_manifest));
  // An inconsistent summary still does not prune: no bound, but no null
  // values either.
  auto no_bounds = ice::PartitionFieldSummary{};
  no_bounds.contains_null = false;
  no_bounds.contains_nan = false;
  auto no_bounds_manifest = ice::ManifestFile{};
  no_bounds_manifest.partitions = {std::move(no_bounds)};
  CHECK(keeps("id == 5", no_bounds_manifest));
  CHECK(keeps("id < 5", no_bounds_manifest));
}

TEST("a required field in the bind schema keeps null checks conservative") {
  // The schema whose names the events carry makes `x` optional; the schema
  // that the scan binds against calls it `y` and requires it.
  auto const resolve = ice::Schema{
    {ice::SchemaField::MakeOptional(1, "x", ice::int64())},
    1,
  };
  auto const strict = ice::Schema{
    {ice::SchemaField::MakeRequired(1, "y", ice::int64())},
    0,
  };
  auto const lenient = ice::Schema{
    {ice::SchemaField::MakeOptional(1, "y", ice::int64())},
    0,
  };
  auto const filter = ir::OptimizeFilter{parse_tql("x == null")};
  // Against the required field, Iceberg could fold the null check to false
  // and prune every file. The translation must not prune anything.
  auto const conservative = translate_filter(filter, resolve, strict);
  REQUIRE(conservative);
  CHECK_EQUAL(conservative->op(), ice::Expression::Operation::kTrue);
  // With the same nullability, the check binds under the other name.
  auto const renamed = translate_filter(filter, resolve, lenient);
  REQUIRE(renamed);
  CHECK_EQUAL(renamed->op(), ice::Expression::Operation::kIsNull);
  CHECK(renamed->ToString().contains("y"));
}

TEST("a required struct in the bind schema keeps nested null checks "
     "conservative") {
  // `s` is optional where the events get their names, but required in the
  // schema that the scan binds against. Its leaf `x` is optional in both.
  auto const make = [](bool struct_optional, std::string_view leaf_name) {
    auto inner
      = std::make_shared<ice::StructType>(std::vector<ice::SchemaField>{
        ice::SchemaField::MakeOptional(2, std::string{leaf_name}, ice::int64()),
      });
    return ice::Schema{
      {struct_optional ? ice::SchemaField::MakeOptional(1, "s", inner)
                       : ice::SchemaField::MakeRequired(1, "s", inner)},
      0,
    };
  };
  auto const resolve = make(true, "x");
  auto const strict = make(false, "y");
  auto const lenient = make(true, "y");
  auto const filter = ir::OptimizeFilter{parse_tql("s.x == null")};
  auto const conservative = translate_filter(filter, resolve, strict);
  REQUIRE(conservative);
  CHECK_EQUAL(conservative->op(), ice::Expression::Operation::kTrue);
  auto const bound = translate_filter(filter, resolve, lenient);
  REQUIRE(bound);
  CHECK_EQUAL(bound->op(), ice::Expression::Operation::kIsNull);
  CHECK(bound->ToString().contains("s.y"));
}

} // namespace

} // namespace tenzir::plugins::iceberg
