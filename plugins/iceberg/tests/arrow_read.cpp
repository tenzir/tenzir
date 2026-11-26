//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/detail/arrow_read.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/data.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/option.hpp>
#include <tenzir/test/test.hpp>

#include <arrow/array/array_nested.h>
#include <arrow/array/util.h>
#include <arrow/buffer.h>
#include <arrow/buffer_builder.h>
#include <arrow/builder.h>
#include <arrow/extension/uuid.h>
#include <arrow/extension_type.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>
#include <arrow/util/key_value_metadata.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

auto make_blob(std::initializer_list<uint8_t> bytes) -> blob {
  auto result = blob{};
  result.reserve(bytes.size());
  for (auto byte : bytes) {
    result.push_back(static_cast<std::byte>(byte));
  }
  return result;
}

auto row_data(ArrowReadBatch const& batch, nova::storage::Index row) -> data {
  return nova::materialize_legacy(batch.events.data.get(row));
}

auto import_ok(std::shared_ptr<arrow::RecordBatch> batch,
               ArrowReadOptions const& options = {}) -> ArrowReadBatch {
  auto result = import_iceberg_batch(std::move(batch), options);
  REQUIRE(result.is_ok());
  return std::move(result).unwrap();
}

auto make_int64_array(std::initializer_list<Option<int64_t>> values)
  -> std::shared_ptr<arrow::Array> {
  auto builder = arrow::Int64Builder{};
  for (auto value : values) {
    tenzir::check(value ? builder.Append(*value) : builder.AppendNull());
  }
  return tenzir::check(builder.Finish());
}

auto make_int32_array(std::initializer_list<Option<int32_t>> values)
  -> std::shared_ptr<arrow::Array> {
  auto builder = arrow::Int32Builder{};
  for (auto value : values) {
    tenzir::check(value ? builder.Append(*value) : builder.AppendNull());
  }
  return tenzir::check(builder.Finish());
}

auto make_utf8_array(std::initializer_list<Option<std::string_view>> values)
  -> std::shared_ptr<arrow::Array> {
  auto builder = arrow::StringBuilder{};
  for (auto value : values) {
    tenzir::check(value ? builder.Append(*value) : builder.AppendNull());
  }
  return tenzir::check(builder.Finish());
}

auto make_large_utf8_array(
  std::initializer_list<Option<std::string_view>> values)
  -> std::shared_ptr<arrow::Array> {
  auto builder = arrow::LargeStringBuilder{};
  for (auto value : values) {
    tenzir::check(value ? builder.Append(*value) : builder.AppendNull());
  }
  return tenzir::check(builder.Finish());
}

auto make_date32_array(std::initializer_list<Option<int32_t>> values)
  -> std::shared_ptr<arrow::Array> {
  auto builder = arrow::Date32Builder{};
  for (auto value : values) {
    tenzir::check(value ? builder.Append(*value) : builder.AppendNull());
  }
  return tenzir::check(builder.Finish());
}

auto make_fixed_binary_array(
  int32_t width, std::vector<Option<std::vector<uint8_t>>> const& values)
  -> std::shared_ptr<arrow::Array> {
  auto builder = arrow::FixedSizeBinaryBuilder{arrow::fixed_size_binary(width)};
  for (auto const& value : values) {
    if (value) {
      REQUIRE_EQUAL(static_cast<int32_t>(value->size()), width);
      tenzir::check(builder.Append(value->data()));
    } else {
      tenzir::check(builder.AppendNull());
    }
  }
  return tenzir::check(builder.Finish());
}

auto make_uuid_metadata() -> std::shared_ptr<arrow::KeyValueMetadata> {
  return arrow::key_value_metadata({"ARROW:extension:name"}, {"arrow.uuid"});
}

auto make_validity_bitmap(std::initializer_list<bool> valid)
  -> std::shared_ptr<arrow::Buffer> {
  auto builder = arrow::TypedBufferBuilder<bool>{};
  for (auto bit : valid) {
    tenzir::check(builder.Append(bit));
  }
  return tenzir::check(builder.Finish());
}

} // namespace

TEST("null, zero-row, and zero-column batches preserve cardinality and name") {
  auto null_result = import_iceberg_batch(nullptr, {});
  CHECK(null_result.is_err());
  // Zero-row batch with typed columns.
  auto empty_schema = arrow::schema({
    arrow::field("id", arrow::int64()),
    arrow::field("d", arrow::date32()),
  });
  auto empty_batch = arrow::RecordBatch::Make(
    empty_schema, 0,
    {tenzir::check(arrow::MakeArrayOfNull(arrow::int64(), 0)),
     tenzir::check(arrow::MakeArrayOfNull(arrow::date32(), 0))});
  auto empty_imported
    = import_ok(std::move(empty_batch), {.schema_name = "iceberg.empty"});
  CHECK(empty_imported.warnings.empty());
  CHECK_EQUAL(empty_imported.events.length(), 0);
  CHECK_EQUAL(empty_imported.events.active_count(), 0);
  CHECK_EQUAL(empty_imported.events.data.length(), 0);
  CHECK_EQUAL(empty_imported.events.mask.length(), 0);
  // Zero-column batch with non-zero rows preserves row count and schema name.
  auto zero_col_batch
    = arrow::RecordBatch::Make(arrow::schema({}), 3, arrow::ArrayVector{});
  auto zero_col = import_ok(std::move(zero_col_batch),
                            {.schema_name = "iceberg.zero_cols"});
  CHECK(zero_col.warnings.empty());
  REQUIRE_EQUAL(zero_col.events.length(), 3);
  CHECK_EQUAL(zero_col.events.active_count(), 3);
  for (auto i = nova::storage::Index{0}; i < 3; ++i) {
    CHECK_EQUAL(row_data(zero_col, i), data{record{}});
    CHECK_EQUAL(*zero_col.events.meta.name.get(i), "iceberg.zero_cols");
  }
}

TEST("fast-path primitive and nested types adopt buffers zero-copy") {
  auto bool_builder = arrow::BooleanBuilder{};
  tenzir::check(bool_builder.Append(true));
  tenzir::check(bool_builder.AppendNull());
  auto i8_builder = arrow::Int8Builder{};
  tenzir::check(i8_builder.Append(-8));
  tenzir::check(i8_builder.Append(8));
  auto i16_builder = arrow::Int16Builder{};
  tenzir::check(i16_builder.Append(-16));
  tenzir::check(i16_builder.Append(16));
  auto i32_builder = arrow::Int32Builder{};
  tenzir::check(i32_builder.Append(-32));
  tenzir::check(i32_builder.Append(32));
  auto i64_col = make_int64_array({100, 200});
  auto u8_builder = arrow::UInt8Builder{};
  tenzir::check(u8_builder.Append(1));
  tenzir::check(u8_builder.Append(2));
  auto u16_builder = arrow::UInt16Builder{};
  tenzir::check(u16_builder.Append(3));
  tenzir::check(u16_builder.Append(4));
  auto u32_builder = arrow::UInt32Builder{};
  tenzir::check(u32_builder.Append(5));
  tenzir::check(u32_builder.Append(6));
  auto u64_builder = arrow::UInt64Builder{};
  tenzir::check(u64_builder.Append(7));
  tenzir::check(u64_builder.Append(8));
  auto f32_builder = arrow::FloatBuilder{};
  tenzir::check(f32_builder.Append(1.5f));
  tenzir::check(f32_builder.Append(-2.5f));
  auto f64_builder = arrow::DoubleBuilder{};
  tenzir::check(f64_builder.Append(3.25));
  tenzir::check(f64_builder.AppendNull());
  auto str_col = make_utf8_array({"alpha", "beta"});
  auto bin_builder = arrow::BinaryBuilder{};
  tenzir::check(bin_builder.Append("\x01\x02", 2));
  tenzir::check(bin_builder.AppendNull());
  auto ts_ns_builder
    = arrow::TimestampBuilder{arrow::timestamp(arrow::TimeUnit::NANO, "UTC"),
                              arrow::default_memory_pool()};
  tenzir::check(ts_ns_builder.Append(1'000'000'000));
  tenzir::check(ts_ns_builder.AppendNull());
  auto null_col = std::make_shared<arrow::NullArray>(2);
  auto list_values = std::make_shared<arrow::Int32Builder>();
  auto list_builder
    = arrow::ListBuilder{arrow::default_memory_pool(), list_values};
  tenzir::check(list_builder.Append());
  tenzir::check(list_values->Append(10));
  tenzir::check(list_values->Append(20));
  tenzir::check(list_builder.Append());
  auto nested_struct = tenzir::check(arrow::StructArray::Make(
    {make_int32_array({7, 8})}, {arrow::field("k", arrow::int32())}));
  auto const* original_i64 = i64_col->data()->buffers[1]->data();
  auto const* original_str = str_col->data()->buffers[2]->data();
  auto schema = arrow::schema({
    arrow::field("b", arrow::boolean()),
    arrow::field("i8", arrow::int8()),
    arrow::field("i16", arrow::int16()),
    arrow::field("i32", arrow::int32()),
    arrow::field("i64", arrow::int64()),
    arrow::field("u8", arrow::uint8()),
    arrow::field("u16", arrow::uint16()),
    arrow::field("u32", arrow::uint32()),
    arrow::field("u64", arrow::uint64()),
    arrow::field("f32", arrow::float32()),
    arrow::field("f64", arrow::float64()),
    arrow::field("s", arrow::utf8()),
    arrow::field("bin", arrow::binary()),
    arrow::field("ts_ns", arrow::timestamp(arrow::TimeUnit::NANO, "UTC")),
    arrow::field("na", arrow::null()),
    arrow::field("xs", arrow::list(arrow::int32())),
    arrow::field("sub", nested_struct->type()),
  });
  auto batch = arrow::RecordBatch::Make(
    schema, 2,
    {tenzir::check(bool_builder.Finish()), tenzir::check(i8_builder.Finish()),
     tenzir::check(i16_builder.Finish()), tenzir::check(i32_builder.Finish()),
     std::move(i64_col), tenzir::check(u8_builder.Finish()),
     tenzir::check(u16_builder.Finish()), tenzir::check(u32_builder.Finish()),
     tenzir::check(u64_builder.Finish()), tenzir::check(f32_builder.Finish()),
     tenzir::check(f64_builder.Finish()), std::move(str_col),
     tenzir::check(bin_builder.Finish()), tenzir::check(ts_ns_builder.Finish()),
     std::move(null_col), tenzir::check(list_builder.Finish()),
     std::move(nested_struct)});
  auto imported = import_ok(std::move(batch), {.schema_name = "iceberg.fast"});
  CHECK(imported.warnings.empty());
  REQUIRE_EQUAL(imported.events.length(), 2);
  CHECK_EQUAL(*imported.events.meta.name.get(0), "iceberg.fast");
  CHECK_EQUAL(*imported.events.meta.name.get(1), "iceberg.fast");
  // Verify zero-copy buffer adoption on untouched fast-path columns.
  auto i64_field = imported.events.data.field("i64");
  REQUIRE(i64_field);
  auto i64_vals = i64_field->data.try_as<nova::Int>();
  REQUIRE(i64_vals);
  auto const& i64_storage
    = as<nova::storage::SparseStorage<int64_t>>(i64_vals->storage());
  CHECK_EQUAL(static_cast<void const*>(i64_storage.data()), original_i64);
  auto str_field = imported.events.data.field("s");
  REQUIRE(str_field);
  auto str_vals = str_field->data.try_as<nova::String>();
  REQUIRE(str_vals);
  auto const& str_storage
    = as<nova::Type<nova::String>::PrimaryPhysicalStorage>(str_vals->storage());
  CHECK_EQUAL(static_cast<void const*>(str_storage.data().begin()),
              original_str);
  CHECK_EQUAL(row_data(imported, 0),
              (data{record{
                {"b", true},
                {"i8", int64_t{-8}},
                {"i16", int64_t{-16}},
                {"i32", int64_t{-32}},
                {"i64", int64_t{100}},
                {"u8", uint64_t{1}},
                {"u16", uint64_t{3}},
                {"u32", uint64_t{5}},
                {"u64", uint64_t{7}},
                {"f32", 1.5},
                {"f64", 3.25},
                {"s", "alpha"},
                {"bin", make_blob({0x01, 0x02})},
                {"ts_ns", tenzir::time{duration{1'000'000'000}}},
                {"na", data{}},
                {"xs", list{int64_t{10}, int64_t{20}}},
                {"sub", record{{"k", int64_t{7}}}},
              }}));
  CHECK_EQUAL(row_data(imported, 1), (data{record{
                                       {"b", data{}},
                                       {"i8", int64_t{8}},
                                       {"i16", int64_t{16}},
                                       {"i32", int64_t{32}},
                                       {"i64", int64_t{200}},
                                       {"u8", uint64_t{2}},
                                       {"u16", uint64_t{4}},
                                       {"u32", uint64_t{6}},
                                       {"u64", uint64_t{8}},
                                       {"f32", -2.5},
                                       {"f64", data{}},
                                       {"s", "beta"},
                                       {"bin", data{}},
                                       {"ts_ns", data{}},
                                       {"na", data{}},
                                       {"xs", list{}},
                                       {"sub", record{{"k", int64_t{8}}}},
                                     }}));
}

TEST("mixed batch adopts fast-path columns zero-copy alongside normalized "
     "columns") {
  auto i64_col = make_int64_array({10, 20});
  auto const* original_i64 = i64_col->data()->buffers[1]->data();
  auto batch
    = arrow::RecordBatch::Make(arrow::schema({
                                 arrow::field("n", arrow::int64()),
                                 arrow::field("d", arrow::date32()),
                               }),
                               2,
                               {std::move(i64_col), make_date32_array({0, 1})});
  auto imported = import_ok(std::move(batch), {.schema_name = "iceberg.mixed"});
  CHECK(imported.warnings.empty());
  auto n_field = imported.events.data.field("n");
  REQUIRE(n_field);
  auto n_vals = n_field->data.try_as<nova::Int>();
  REQUIRE(n_vals);
  auto const& n_storage
    = as<nova::storage::SparseStorage<int64_t>>(n_vals->storage());
  CHECK_EQUAL(static_cast<void const*>(n_storage.data()), original_i64);
  CHECK_EQUAL(row_data(imported, 0), (data{record{
                                       {"n", int64_t{10}},
                                       {"d", tenzir::time{duration{0}}},
                                     }}));
  CHECK_EQUAL(row_data(imported, 1),
              (data{record{
                {"n", int64_t{20}},
                {"d", tenzir::time{std::chrono::days{1}}},
              }}));
}

TEST("decimal128 and decimal256 respect string and float formats with slices") {
  auto d128_builder = arrow::Decimal128Builder{arrow::decimal128(10, 2)};
  tenzir::check(d128_builder.Append(arrow::Decimal128{12345}));
  tenzir::check(d128_builder.Append(arrow::Decimal128{-50}));
  tenzir::check(d128_builder.AppendNull());
  tenzir::check(d128_builder.Append(arrow::Decimal128{9999999}));
  auto d128 = tenzir::check(d128_builder.Finish());
  auto d256_builder = arrow::Decimal256Builder{arrow::decimal256(50, 2)};
  tenzir::check(d256_builder.Append(tenzir::check(
    arrow::Decimal256::FromString("123456789012345678901234567890.25"))));
  tenzir::check(d256_builder.AppendNull());
  tenzir::check(d256_builder.Append(arrow::Decimal256{-4275}));
  tenzir::check(d256_builder.Append(arrow::Decimal256{0}));
  auto d256 = tenzir::check(d256_builder.Finish());
  auto nested = tenzir::check(arrow::StructArray::Make(
    {d128}, {arrow::field("amount", arrow::decimal128(10, 2))}));
  auto schema = arrow::schema({
    arrow::field("d128", arrow::decimal128(10, 2)),
    arrow::field("d256", arrow::decimal256(50, 2)),
    arrow::field("nested", nested->type()),
  });
  auto batch = arrow::RecordBatch::Make(schema, 4, {d128, d256, nested});
  // DecimalFormat::string.
  auto as_string = import_ok(batch, {.decimal_format = DecimalFormat::string,
                                     .schema_name = ""});
  CHECK(as_string.warnings.empty());
  CHECK_EQUAL(row_data(as_string, 0),
              (data{record{
                {"d128", "123.45"},
                {"d256", "123456789012345678901234567890.25"},
                {"nested", record{{"amount", "123.45"}}},
              }}));
  CHECK_EQUAL(row_data(as_string, 1), (data{record{
                                        {"d128", "-0.50"},
                                        {"d256", data{}},
                                        {"nested", record{{"amount", "-0.50"}}},
                                      }}));
  CHECK_EQUAL(row_data(as_string, 2), (data{record{
                                        {"d128", data{}},
                                        {"d256", "-42.75"},
                                        {"nested", record{{"amount", data{}}}},
                                      }}));
  CHECK_EQUAL(row_data(as_string, 3),
              (data{record{
                {"d128", "99999.99"},
                {"d256", "0.00"},
                {"nested", record{{"amount", "99999.99"}}},
              }}));
  // DecimalFormat::float_.
  auto as_float = import_ok(batch, {.decimal_format = DecimalFormat::float_,
                                    .schema_name = ""});
  CHECK(as_float.warnings.empty());
  CHECK_EQUAL(row_data(as_float, 1), (data{record{
                                       {"d128", -0.5},
                                       {"d256", data{}},
                                       {"nested", record{{"amount", -0.5}}},
                                     }}));
  CHECK_EQUAL(row_data(as_float, 2), (data{record{
                                       {"d128", data{}},
                                       {"d256", -42.75},
                                       {"nested", record{{"amount", data{}}}},
                                     }}));
  CHECK_EQUAL(row_data(as_float, 3), (data{record{
                                       {"d128", 99999.99},
                                       {"d256", 0.0},
                                       {"nested", record{{"amount", 99999.99}}},
                                     }}));
  // Sliced decimal batch.
  auto sliced
    = import_ok(batch->Slice(1, 2),
                {.decimal_format = DecimalFormat::string, .schema_name = ""});
  REQUIRE_EQUAL(sliced.events.length(), 2);
  CHECK_EQUAL(row_data(sliced, 0), row_data(as_string, 1));
  CHECK_EQUAL(row_data(sliced, 1), row_data(as_string, 2));
}

TEST("date32 converts to midnight UTC and warns once on out-of-range dates") {
  // 2932896 is 9999-12-31, -719162 is 0001-01-01; both exceed int64 nanos.
  auto dates = make_date32_array({0, 19723, -1, None{}, 2932896, -719162});
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({arrow::field("event_date", arrow::date32())}), 6, {dates});
  auto imported = import_ok(batch);
  REQUIRE_EQUAL(imported.events.length(), 6);
  CHECK_EQUAL(row_data(imported, 0),
              (data{record{{"event_date", tenzir::time{duration{0}}}}}));
  CHECK_EQUAL(
    row_data(imported, 1),
    (data{record{{"event_date", tenzir::time{std::chrono::days{19723}}}}}));
  CHECK_EQUAL(
    row_data(imported, 2),
    (data{record{{"event_date", tenzir::time{std::chrono::days{-1}}}}}));
  CHECK_EQUAL(row_data(imported, 3), (data{record{{"event_date", data{}}}}));
  CHECK_EQUAL(row_data(imported, 4), (data{record{{"event_date", data{}}}}));
  CHECK_EQUAL(row_data(imported, 5), (data{record{{"event_date", data{}}}}));
  REQUIRE_EQUAL(imported.warnings.size(), size_t{1});
  CHECK(imported.warnings[0].contains("event_date"));
  // Slicing out the out-of-range rows produces no warning.
  auto in_range = import_ok(batch->Slice(0, 4));
  CHECK(in_range.warnings.empty());
  CHECK_EQUAL(row_data(in_range, 1), row_data(imported, 1));
}

TEST("time64 and time32 convert to duration since midnight") {
  auto t64_us = arrow::Time64Builder{arrow::time64(arrow::TimeUnit::MICRO),
                                     arrow::default_memory_pool()};
  tenzir::check(t64_us.Append(0));
  tenzir::check(t64_us.Append(3'661'000'500));
  tenzir::check(t64_us.AppendNull());
  auto t64_ns = arrow::Time64Builder{arrow::time64(arrow::TimeUnit::NANO),
                                     arrow::default_memory_pool()};
  tenzir::check(t64_ns.Append(123'456'789));
  tenzir::check(t64_ns.AppendNull());
  tenzir::check(t64_ns.Append(42));
  auto t32_s = arrow::Time32Builder{arrow::time32(arrow::TimeUnit::SECOND),
                                    arrow::default_memory_pool()};
  tenzir::check(t32_s.Append(3600));
  tenzir::check(t32_s.Append(0));
  tenzir::check(t32_s.AppendNull());
  auto t32_ms = arrow::Time32Builder{arrow::time32(arrow::TimeUnit::MILLI),
                                     arrow::default_memory_pool()};
  tenzir::check(t32_ms.Append(1500));
  tenzir::check(t32_ms.AppendNull());
  tenzir::check(t32_ms.Append(250));
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({
      arrow::field("t64_us", arrow::time64(arrow::TimeUnit::MICRO)),
      arrow::field("t64_ns", arrow::time64(arrow::TimeUnit::NANO)),
      arrow::field("t32_s", arrow::time32(arrow::TimeUnit::SECOND)),
      arrow::field("t32_ms", arrow::time32(arrow::TimeUnit::MILLI)),
    }),
    3,
    {tenzir::check(t64_us.Finish()), tenzir::check(t64_ns.Finish()),
     tenzir::check(t32_s.Finish()), tenzir::check(t32_ms.Finish())});
  auto imported = import_ok(batch);
  CHECK(imported.warnings.empty());
  CHECK_EQUAL(row_data(imported, 0),
              (data{record{
                {"t64_us", duration{0}},
                {"t64_ns", duration{123'456'789}},
                {"t32_s", duration{std::chrono::seconds{3600}}},
                {"t32_ms", duration{std::chrono::milliseconds{1500}}},
              }}));
  CHECK_EQUAL(row_data(imported, 1),
              (data{record{
                {"t64_us", duration{std::chrono::microseconds{3'661'000'500}}},
                {"t64_ns", data{}},
                {"t32_s", duration{0}},
                {"t32_ms", data{}},
              }}));
  CHECK_EQUAL(row_data(imported, 2),
              (data{record{
                {"t64_us", data{}},
                {"t64_ns", duration{42}},
                {"t32_s", data{}},
                {"t32_ms", duration{std::chrono::milliseconds{250}}},
              }}));
  // Sliced time batch preserves offsets.
  auto sliced = import_ok(batch->Slice(1, 2));
  REQUIRE_EQUAL(sliced.events.length(), 2);
  CHECK_EQUAL(row_data(sliced, 0), row_data(imported, 1));
  CHECK_EQUAL(row_data(sliced, 1), row_data(imported, 2));
}

TEST("non-nanosecond timestamps convert safely and warn on overflow") {
  auto ts_us_builder = arrow::TimestampBuilder{
    arrow::timestamp(arrow::TimeUnit::MICRO), arrow::default_memory_pool()};
  tenzir::check(ts_us_builder.Append(1'704'067'200'123'456));
  tenzir::check(ts_us_builder.Append(-1'000'000));
  tenzir::check(ts_us_builder.AppendNull());
  // 9999-12-31T23:59:59.999999Z and 0001-01-01T00:00:00Z overflow int64 ns.
  tenzir::check(ts_us_builder.Append(253'402'300'799'999'999));
  tenzir::check(ts_us_builder.Append(-62'135'596'800'000'000));
  auto ts_tz_builder
    = arrow::TimestampBuilder{arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                              arrow::default_memory_pool()};
  tenzir::check(ts_tz_builder.Append(1'000));
  tenzir::check(ts_tz_builder.Append(2'000));
  tenzir::check(ts_tz_builder.AppendNull());
  tenzir::check(ts_tz_builder.Append(3'000));
  tenzir::check(ts_tz_builder.Append(4'000));
  auto ts_ms_builder = arrow::TimestampBuilder{
    arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool()};
  tenzir::check(ts_ms_builder.Append(5'000));
  tenzir::check(ts_ms_builder.Append(std::numeric_limits<int64_t>::max()));
  tenzir::check(ts_ms_builder.AppendNull());
  tenzir::check(ts_ms_builder.Append(-5'000));
  tenzir::check(ts_ms_builder.Append(0));
  auto ts_s_builder = arrow::TimestampBuilder{
    arrow::timestamp(arrow::TimeUnit::SECOND), arrow::default_memory_pool()};
  tenzir::check(ts_s_builder.Append(10));
  tenzir::check(ts_s_builder.Append(-10));
  tenzir::check(ts_s_builder.AppendNull());
  tenzir::check(ts_s_builder.Append(0));
  tenzir::check(ts_s_builder.Append(std::numeric_limits<int64_t>::lowest()));
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({
      arrow::field("ts_us", arrow::timestamp(arrow::TimeUnit::MICRO)),
      arrow::field("ts_tz", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
      arrow::field("ts_ms", arrow::timestamp(arrow::TimeUnit::MILLI)),
      arrow::field("ts_s", arrow::timestamp(arrow::TimeUnit::SECOND)),
    }),
    5,
    {tenzir::check(ts_us_builder.Finish()),
     tenzir::check(ts_tz_builder.Finish()),
     tenzir::check(ts_ms_builder.Finish()),
     tenzir::check(ts_s_builder.Finish())});
  auto imported = import_ok(std::move(batch));
  REQUIRE_EQUAL(imported.events.length(), 5);
  CHECK_EQUAL(
    row_data(imported, 0),
    (data{record{
      {"ts_us", tenzir::time{std::chrono::microseconds{1'704'067'200'123'456}}},
      {"ts_tz", tenzir::time{std::chrono::microseconds{1'000}}},
      {"ts_ms", tenzir::time{std::chrono::milliseconds{5'000}}},
      {"ts_s", tenzir::time{std::chrono::seconds{10}}},
    }}));
  CHECK_EQUAL(row_data(imported, 1),
              (data{record{
                {"ts_us", tenzir::time{std::chrono::microseconds{-1'000'000}}},
                {"ts_tz", tenzir::time{std::chrono::microseconds{2'000}}},
                {"ts_ms", data{}},
                {"ts_s", tenzir::time{std::chrono::seconds{-10}}},
              }}));
  CHECK_EQUAL(row_data(imported, 2), (data{record{
                                       {"ts_us", data{}},
                                       {"ts_tz", data{}},
                                       {"ts_ms", data{}},
                                       {"ts_s", data{}},
                                     }}));
  CHECK_EQUAL(row_data(imported, 3),
              (data{record{
                {"ts_us", data{}},
                {"ts_tz", tenzir::time{std::chrono::microseconds{3'000}}},
                {"ts_ms", tenzir::time{std::chrono::milliseconds{-5'000}}},
                {"ts_s", tenzir::time{duration{0}}},
              }}));
  CHECK_EQUAL(row_data(imported, 4),
              (data{record{
                {"ts_us", data{}},
                {"ts_tz", tenzir::time{std::chrono::microseconds{4'000}}},
                {"ts_ms", tenzir::time{duration{0}}},
                {"ts_s", data{}},
              }}));
  // One warning per overflowing column: ts_us, ts_ms, ts_s (not ts_tz).
  REQUIRE_EQUAL(imported.warnings.size(), size_t{3});
  CHECK(imported.warnings[0].contains("ts_us"));
  CHECK(imported.warnings[1].contains("ts_ms"));
  CHECK(imported.warnings[2].contains("ts_s"));
}

TEST("UUID columns format as lowercase hyphenated strings in both forms") {
  auto uuid_a
    = std::vector<uint8_t>{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                           0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef};
  auto uuid_zero = std::vector<uint8_t>(16, 0x00);
  auto uuid_ff = std::vector<uint8_t>(16, 0xff);
  auto storage
    = make_fixed_binary_array(16, {uuid_a, None{}, uuid_zero, uuid_ff});
  auto ext_uuid
    = arrow::ExtensionType::WrapArray(arrow::extension::uuid(), storage);
  auto meta_uuid
    = make_fixed_binary_array(16, {uuid_ff, uuid_a, None{}, uuid_zero});
  auto nested = tenzir::check(arrow::StructArray::Make(
    {ext_uuid, meta_uuid}, {arrow::field("ext", arrow::extension::uuid()),
                            arrow::field("meta", arrow::fixed_size_binary(16),
                                         true, make_uuid_metadata())}));
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({
      arrow::field("ext_uuid", arrow::extension::uuid()),
      arrow::field("meta_uuid", arrow::fixed_size_binary(16), true,
                   make_uuid_metadata()),
      arrow::field("nested", nested->type()),
    }),
    4, {ext_uuid, meta_uuid, nested});
  auto imported = import_ok(batch);
  CHECK(imported.warnings.empty());
  CHECK_EQUAL(row_data(imported, 0),
              (data{record{
                {"ext_uuid", "01234567-89ab-cdef-0123-456789abcdef"},
                {"meta_uuid", "ffffffff-ffff-ffff-ffff-ffffffffffff"},
                {"nested",
                 record{
                   {"ext", "01234567-89ab-cdef-0123-456789abcdef"},
                   {"meta", "ffffffff-ffff-ffff-ffff-ffffffffffff"},
                 }},
              }}));
  CHECK_EQUAL(row_data(imported, 1),
              (data{record{
                {"ext_uuid", data{}},
                {"meta_uuid", "01234567-89ab-cdef-0123-456789abcdef"},
                {"nested",
                 record{
                   {"ext", data{}},
                   {"meta", "01234567-89ab-cdef-0123-456789abcdef"},
                 }},
              }}));
  CHECK_EQUAL(row_data(imported, 2),
              (data{record{
                {"ext_uuid", "00000000-0000-0000-0000-000000000000"},
                {"meta_uuid", data{}},
                {"nested",
                 record{
                   {"ext", "00000000-0000-0000-0000-000000000000"},
                   {"meta", data{}},
                 }},
              }}));
  CHECK_EQUAL(row_data(imported, 3),
              (data{record{
                {"ext_uuid", "ffffffff-ffff-ffff-ffff-ffffffffffff"},
                {"meta_uuid", "00000000-0000-0000-0000-000000000000"},
                {"nested",
                 record{
                   {"ext", "ffffffff-ffff-ffff-ffff-ffffffffffff"},
                   {"meta", "00000000-0000-0000-0000-000000000000"},
                 }},
              }}));
  // Sliced UUID batch preserves offsets and nulls.
  auto sliced = import_ok(batch->Slice(1, 2));
  REQUIRE_EQUAL(sliced.events.length(), 2);
  CHECK_EQUAL(row_data(sliced, 0), row_data(imported, 1));
  CHECK_EQUAL(row_data(sliced, 1), row_data(imported, 2));
}

TEST("fixed_size_binary without UUID metadata converts to blob") {
  auto fsb4 = make_fixed_binary_array(
    4, {std::vector<uint8_t>{0xde, 0xad, 0xbe, 0xef}, None{},
        std::vector<uint8_t>{0x00, 0x01, 0x02, 0x03}});
  auto raw16_bytes
    = std::vector<uint8_t>{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                           0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef};
  auto fsb16 = make_fixed_binary_array(16, {raw16_bytes, raw16_bytes, None{}});
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({
      arrow::field("tag", arrow::fixed_size_binary(4)),
      arrow::field("raw16", arrow::fixed_size_binary(16)),
    }),
    3, {fsb4, fsb16});
  auto imported = import_ok(batch);
  CHECK(imported.warnings.empty());
  auto expected_raw16
    = make_blob({0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23,
                 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef});
  CHECK_EQUAL(row_data(imported, 0),
              (data{record{
                {"tag", make_blob({0xde, 0xad, 0xbe, 0xef})},
                {"raw16", expected_raw16},
              }}));
  CHECK_EQUAL(row_data(imported, 1), (data{record{
                                       {"tag", data{}},
                                       {"raw16", expected_raw16},
                                     }}));
  CHECK_EQUAL(row_data(imported, 2),
              (data{record{
                {"tag", make_blob({0x00, 0x01, 0x02, 0x03})},
                {"raw16", data{}},
              }}));
  auto sliced = import_ok(batch->Slice(1, 2));
  REQUIRE_EQUAL(sliced.events.length(), 2);
  CHECK_EQUAL(row_data(sliced, 0), row_data(imported, 1));
  CHECK_EQUAL(row_data(sliced, 1), row_data(imported, 2));
}

TEST("large_string, large_binary, and large_list convert with slices and "
     "nulls") {
  auto lstr = make_large_utf8_array({"prefix", "hello", "", None{}, "world"});
  auto lbin_builder = arrow::LargeBinaryBuilder{};
  tenzir::check(lbin_builder.Append("skip", 4));
  tenzir::check(lbin_builder.Append("\x01\x00\x02", 3));
  tenzir::check(lbin_builder.Append("", 0));
  tenzir::check(lbin_builder.AppendNull());
  tenzir::check(lbin_builder.Append("\xff", 1));
  auto lbin = tenzir::check(lbin_builder.Finish());
  auto date_values = std::make_shared<arrow::Date32Builder>();
  auto llist_builder
    = arrow::LargeListBuilder{arrow::default_memory_pool(), date_values};
  // Row 0 (sliced out).
  tenzir::check(llist_builder.Append());
  tenzir::check(date_values->Append(999));
  // Row 1: [1970-01-01, null, 1970-01-02].
  tenzir::check(llist_builder.Append());
  tenzir::check(date_values->Append(0));
  tenzir::check(date_values->AppendNull());
  tenzir::check(date_values->Append(1));
  // Row 2: empty list.
  tenzir::check(llist_builder.Append());
  // Row 3: null list.
  tenzir::check(llist_builder.AppendNull());
  // Row 4: [1970-01-03].
  tenzir::check(llist_builder.Append());
  tenzir::check(date_values->Append(2));
  auto llist = tenzir::check(llist_builder.Finish());
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({
      arrow::field("ls", arrow::large_utf8()),
      arrow::field("lb", arrow::large_binary()),
      arrow::field("ll", arrow::large_list(arrow::date32())),
    }),
    5, {lstr, lbin, llist});
  auto sliced = import_ok(batch->Slice(1, 4));
  CHECK(sliced.warnings.empty());
  REQUIRE_EQUAL(sliced.events.length(), 4);
  CHECK_EQUAL(row_data(sliced, 0),
              (data{record{
                {"ls", "hello"},
                {"lb", make_blob({0x01, 0x00, 0x02})},
                {"ll", list{tenzir::time{duration{0}}, data{},
                            tenzir::time{std::chrono::days{1}}}},
              }}));
  CHECK_EQUAL(row_data(sliced, 1), (data{record{
                                     {"ls", ""},
                                     {"lb", blob{}},
                                     {"ll", list{}},
                                   }}));
  CHECK_EQUAL(row_data(sliced, 2), (data{record{
                                     {"ls", data{}},
                                     {"lb", data{}},
                                     {"ll", data{}},
                                   }}));
  CHECK_EQUAL(row_data(sliced, 3),
              (data{record{
                {"ls", "world"},
                {"lb", make_blob({0xff})},
                {"ll", list{tenzir::time{std::chrono::days{2}}}},
              }}));
}

TEST("string-keyed maps reshape into records for arbitrary value types") {
  // Map<utf8, int64>: regular, null map, empty map, null key + duplicate key.
  auto map_type = std::make_shared<arrow::MapType>(
    arrow::field("key", arrow::utf8(), false),
    arrow::field("value", arrow::int64(), true));
  auto offsets = make_int32_array({0, 2, 2, 2, 5});
  auto keys = make_utf8_array({"a", "b", "x", "x", "y"});
  auto items = make_int64_array({1, 2, 10, 20, None{}});
  auto validity = make_validity_bitmap({true, false, true, true});
  auto str_int_map = tenzir::check(arrow::MapArray::FromArrays(
    map_type, offsets, keys, items, arrow::default_memory_pool(), validity));
  // Map<large_utf8, decimal128(10, 2)>.
  auto large_map_type = std::make_shared<arrow::MapType>(
    arrow::field("key", arrow::large_utf8(), false),
    arrow::field("value", arrow::decimal128(10, 2), true));
  auto large_offsets = make_int32_array({0, 1, 1, 2, 2});
  auto large_keys = make_large_utf8_array({"price", "tax"});
  auto dec_builder = arrow::Decimal128Builder{arrow::decimal128(10, 2)};
  tenzir::check(dec_builder.Append(arrow::Decimal128{1999}));
  tenzir::check(dec_builder.Append(arrow::Decimal128{250}));
  auto large_dec_map = tenzir::check(
    arrow::MapArray::FromArrays(large_map_type, large_offsets, large_keys,
                                tenzir::check(dec_builder.Finish())));
  auto batch
    = arrow::RecordBatch::Make(arrow::schema({
                                 arrow::field("m", map_type),
                                 arrow::field("prices", large_map_type),
                               }),
                               4, {str_int_map, large_dec_map});
  auto imported = import_ok(batch, {.decimal_format = DecimalFormat::string,
                                    .schema_name = ""});
  CHECK(imported.warnings.empty());
  REQUIRE_EQUAL(imported.events.length(), 4);
  CHECK_EQUAL(row_data(imported, 0),
              (data{record{
                {"m", record{{"a", int64_t{1}}, {"b", int64_t{2}}}},
                {"prices", record{{"price", "19.99"}}},
              }}));
  CHECK_EQUAL(row_data(imported, 1), (data{record{
                                       {"m", data{}},
                                       {"prices", record{}},
                                     }}));
  CHECK_EQUAL(row_data(imported, 2), (data{record{
                                       {"m", record{}},
                                       {"prices", record{{"tax", "2.50"}}},
                                     }}));
  // Null key is skipped, duplicate key "x" takes the last value (20).
  CHECK_EQUAL(row_data(imported, 3),
              (data{record{
                {"m", record{{"x", int64_t{20}}, {"y", data{}}}},
                {"prices", record{}},
              }}));
  // Float decimal format inside map values + sliced map.
  auto sliced_float
    = import_ok(batch->Slice(2, 2),
                {.decimal_format = DecimalFormat::float_, .schema_name = ""});
  REQUIRE_EQUAL(sliced_float.events.length(), 2);
  CHECK_EQUAL(row_data(sliced_float, 0), (data{record{
                                           {"m", record{}},
                                           {"prices", record{{"tax", 2.5}}},
                                         }}));
  CHECK_EQUAL(row_data(sliced_float, 1),
              (data{record{
                {"m", record{{"x", int64_t{20}}, {"y", data{}}}},
                {"prices", record{}},
              }}));
}

TEST("non-string-keyed maps reshape into lists of key-value records") {
  auto int_map_type = std::make_shared<arrow::MapType>(
    arrow::field("key", arrow::int32(), false),
    arrow::field("value", arrow::utf8(), true));
  auto offsets = make_int32_array({0, 2, 2, 2, 3});
  auto keys = make_int32_array({1, 2, 3});
  auto items = make_utf8_array({"one", "two", None{}});
  auto validity = make_validity_bitmap({true, false, true, true});
  auto int_map = tenzir::check(
    arrow::MapArray::FromArrays(int_map_type, offsets, keys, items,
                                arrow::default_memory_pool(), validity));
  // Map with UUID keys (via field metadata) and int64 values.
  auto uuid_key_field = arrow::field("key", arrow::fixed_size_binary(16), false,
                                     make_uuid_metadata());
  auto uuid_map_type = std::make_shared<arrow::MapType>(
    uuid_key_field, arrow::field("value", arrow::int64(), true));
  auto uuid_bytes
    = std::vector<uint8_t>{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
                           0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef};
  auto uuid_offsets = make_int32_array({0, 1, 1, 1, 1});
  auto uuid_keys = make_fixed_binary_array(16, {uuid_bytes});
  auto uuid_items = make_int64_array({42});
  auto uuid_map = tenzir::check(arrow::MapArray::FromArrays(
    uuid_map_type, uuid_offsets, uuid_keys, uuid_items));
  auto batch
    = arrow::RecordBatch::Make(arrow::schema({
                                 arrow::field("by_id", int_map_type),
                                 arrow::field("by_uuid", uuid_map_type),
                               }),
                               4, {int_map, uuid_map});
  auto imported = import_ok(batch);
  CHECK(imported.warnings.empty());
  REQUIRE_EQUAL(imported.events.length(), 4);
  CHECK_EQUAL(row_data(imported, 0),
              (data{record{
                {"by_id", list{record{{"key", int64_t{1}}, {"value", "one"}},
                               record{{"key", int64_t{2}}, {"value", "two"}}}},
                {"by_uuid", list{record{{"key", "01234567-89ab-cdef-0123-"
                                                "456789abcdef"},
                                        {"value", int64_t{42}}}}},
              }}));
  CHECK_EQUAL(row_data(imported, 1), (data{record{
                                       {"by_id", data{}},
                                       {"by_uuid", list{}},
                                     }}));
  CHECK_EQUAL(row_data(imported, 2), (data{record{
                                       {"by_id", list{}},
                                       {"by_uuid", list{}},
                                     }}));
  CHECK_EQUAL(row_data(imported, 3),
              (data{record{
                {"by_id", list{record{{"key", int64_t{3}}, {"value", data{}}}}},
                {"by_uuid", list{}},
              }}));
  // Sliced non-string map preserves offsets and validity.
  auto sliced = import_ok(batch->Slice(1, 3));
  REQUIRE_EQUAL(sliced.events.length(), 3);
  CHECK_EQUAL(row_data(sliced, 0), row_data(imported, 1));
  CHECK_EQUAL(row_data(sliced, 1), row_data(imported, 2));
  CHECK_EQUAL(row_data(sliced, 2), row_data(imported, 3));
}

TEST("nested structs preserve parent nulls, slices, and hide warnings under "
     "null parents") {
  auto ts_builder
    = arrow::TimestampBuilder{arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                              arrow::default_memory_pool()};
  // Row 0 overflows, but the parent struct at row 0 is null.
  tenzir::check(ts_builder.Append(253'402'300'799'999'999));
  tenzir::check(ts_builder.Append(1'000'000));
  tenzir::check(ts_builder.Append(2'000'000));
  tenzir::check(ts_builder.Append(3'000'000));
  // Row 3 has an out-of-range date under a valid parent struct.
  auto dates = make_date32_array({0, 1, 2, 2932896});
  auto uuid_bytes
    = std::vector<uint8_t>{0xaa, 0xbb, 0xcc, 0xdd, 0x00, 0x11, 0x22, 0x33,
                           0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb};
  auto sub_uuids = make_fixed_binary_array(16, {uuid_bytes, uuid_bytes,
                                                uuid_bytes, uuid_bytes});
  auto sub_vals = make_int32_array({10, 20, 30, 40});
  auto sub_validity = make_validity_bitmap({true, true, false, true});
  auto sub = tenzir::check(
    arrow::StructArray::Make({sub_uuids, sub_vals},
                             {arrow::field("u", arrow::fixed_size_binary(16),
                                           true, make_uuid_metadata()),
                              arrow::field("v", arrow::int32())},
                             sub_validity));
  auto outer_validity = make_validity_bitmap({false, true, true, true});
  auto outer = tenzir::check(arrow::StructArray::Make(
    {tenzir::check(ts_builder.Finish()), dates, sub},
    {arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
     arrow::field("d", arrow::date32()), arrow::field("sub", sub->type())},
    outer_validity));
  auto batch = arrow::RecordBatch::Make(
    arrow::schema({arrow::field("outer", outer->type())}), 4, {outer});
  auto imported = import_ok(batch, {.schema_name = "iceberg.nested"});
  REQUIRE_EQUAL(imported.events.length(), 4);
  CHECK_EQUAL(*imported.events.meta.name.get(0), "iceberg.nested");
  CHECK_EQUAL(row_data(imported, 0), (data{record{{"outer", data{}}}}));
  CHECK_EQUAL(row_data(imported, 1),
              (data{record{
                {"outer",
                 record{
                   {"ts", tenzir::time{std::chrono::seconds{1}}},
                   {"d", tenzir::time{std::chrono::days{1}}},
                   {"sub",
                    record{
                      {"u", "aabbccdd-0011-2233-4455-66778899aabb"},
                      {"v", int64_t{20}},
                    }},
                 }},
              }}));
  CHECK_EQUAL(row_data(imported, 2),
              (data{record{
                {"outer",
                 record{
                   {"ts", tenzir::time{std::chrono::seconds{2}}},
                   {"d", tenzir::time{std::chrono::days{2}}},
                   {"sub", data{}},
                 }},
              }}));
  CHECK_EQUAL(row_data(imported, 3),
              (data{record{
                {"outer",
                 record{
                   {"ts", tenzir::time{std::chrono::seconds{3}}},
                   {"d", data{}},
                   {"sub",
                    record{
                      {"u", "aabbccdd-0011-2233-4455-66778899aabb"},
                      {"v", int64_t{40}},
                    }},
                 }},
              }}));
  // Only outer.d warned; outer.ts at row 0 was hidden beneath a null parent.
  REQUIRE_EQUAL(imported.warnings.size(), size_t{1});
  CHECK(imported.warnings[0].contains("outer.d"));
  // Sliced struct batch preserves validity bitmap offset.
  auto sliced = import_ok(batch->Slice(1, 2));
  CHECK(sliced.warnings.empty());
  REQUIRE_EQUAL(sliced.events.length(), 2);
  CHECK_EQUAL(row_data(sliced, 0), row_data(imported, 1));
  CHECK_EQUAL(row_data(sliced, 1), row_data(imported, 2));
}

} // namespace tenzir::plugins::iceberg
