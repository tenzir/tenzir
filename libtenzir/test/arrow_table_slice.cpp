//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_table_slice.hpp"

#include "tenzir/arrow_memory_pool.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/test/test.hpp"

#include <arrow/api.h>
#include <arrow/util/bit_util.h>

#include <cstring>

namespace tenzir {

TEST("flatten renames exact duplicates but not suffix matches") {
  auto builder = series_builder{};
  auto row = builder.record();
  row.field("x").data(int64_t{1});
  row.field("a.x").data(int64_t{2});
  row.field("a").record().field("x").data(int64_t{3});
  auto slices = builder.finish_as_table_slice("test");
  REQUIRE_EQUAL(slices.size(), 1u);
  auto result = flatten(slices[0]);
  auto const& layout = as<record_type>(result.slice.schema());
  REQUIRE_EQUAL(layout.num_fields(), 3u);
  CHECK_EQUAL(layout.field(0).name, "x");
  CHECK_EQUAL(layout.field(1).name, "a.x");
  CHECK_EQUAL(layout.field(2).name, "a.x_1");
  CHECK_EQUAL(result.renamed_fields,
              (std::vector<std::string>{"a.x -> a.x_1"}));
  CHECK_EQUAL(materialize(result.slice.at(0, 2)), data{int64_t{3}});
}

TEST("record batch from struct array preserves row nulls") {
  auto int_builder = arrow::Int64Builder{arrow_memory_pool()};
  tenzir::check(int_builder.Append(int64_t{42}));
  tenzir::check(int_builder.Append(int64_t{1337}));
  auto ints = finish(int_builder);
  auto string_builder = arrow::StringBuilder{arrow_memory_pool()};
  tenzir::check(string_builder.Append("keep"));
  tenzir::check(string_builder.Append("leak"));
  auto strings = finish(string_builder);
  auto bitmap = tenzir::check(arrow::AllocateBitmap(2, arrow_memory_pool()));
  std::memset(bitmap->mutable_data(), 0, bitmap->size());
  arrow::bit_util::SetBit(bitmap->mutable_data(), 0);
  auto fields = arrow::FieldVector{
    arrow::field("a", arrow::int64()),
    arrow::field("b", arrow::utf8()),
  };
  auto struct_array = make_struct_array(2, bitmap, fields, {ints, strings});
  auto batch
    = record_batch_from_struct_array(arrow::schema(fields), *struct_array);
  REQUIRE(batch);
  auto a
    = std::dynamic_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("a"));
  auto b = std::dynamic_pointer_cast<arrow::StringArray>(
    batch->GetColumnByName("b"));
  REQUIRE(a);
  REQUIRE(b);
  CHECK(not a->IsNull(0));
  CHECK_EQUAL(a->Value(0), int64_t{42});
  CHECK(not b->IsNull(0));
  CHECK_EQUAL(b->GetString(0), "keep");
  CHECK(a->IsNull(1));
  CHECK(b->IsNull(1));
}

TEST("try_from converts foreign timestamp precision") {
  // Batches from files written by other systems commonly carry
  // non-nanosecond timestamps: Spark defaults to microseconds and Iceberg
  // mandates them. try_from must convert both the data and the derived
  // schema to Tenzir's nanosecond time type.
  constexpr auto micros = int64_t{1'637'229'700'158'940};
  auto builder = arrow::TimestampBuilder{
    arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow_memory_pool()};
  tenzir::check(builder.Append(micros));
  auto timestamps = finish(builder);
  auto schema = arrow::schema(
    {arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))});
  auto batch = arrow::RecordBatch::Make(std::move(schema), 1, {timestamps});
  REQUIRE(batch);
  auto slice = table_slice::try_from(batch);
  REQUIRE(slice.has_value());
  const auto& layout = as<record_type>(slice->schema());
  CHECK_EQUAL(layout.field(0).type, type{time_type{}});
  const auto expected = time{} + std::chrono::microseconds{micros};
  CHECK_EQUAL(materialize(slice->at(0, 0)), data{expected});
}

TEST("try_from preserves list value field metadata") {
  constexpr auto micros = int64_t{1'637'229'700'158'940};
  auto builder = arrow::TimestampBuilder{
    arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"), arrow_memory_pool()};
  tenzir::check(builder.Append(micros));
  auto timestamps = finish(builder);
  auto offset_builder = arrow::Int32Builder{arrow_memory_pool()};
  tenzir::check(offset_builder.Append(int32_t{0}));
  tenzir::check(offset_builder.Append(int32_t{1}));
  auto offsets = std::shared_ptr<arrow::Array>{};
  tenzir::check(offset_builder.Finish(&offsets));
  auto value_type = type{"named_time", time_type{}, {{"origin", "test"}}};
  auto value_field
    = arrow::field("item", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"),
                   true, value_type.make_arrow_metadata());
  auto arrow_list_type = arrow::list(std::move(value_field));
  auto list = tenzir::check(arrow::ListArray::FromArrays(
    arrow_list_type, *offsets, *timestamps, arrow_memory_pool()));
  auto schema = arrow::schema({arrow::field("xs", arrow_list_type)});
  auto batch = arrow::RecordBatch::Make(std::move(schema), 1, {list});
  REQUIRE(batch);
  auto slice = table_slice::try_from(batch);
  REQUIRE(slice.has_value());
  const auto& layout = as<record_type>(slice->schema());
  const auto& field_type = layout.field(0).type;
  REQUIRE(is<list_type>(field_type));
  const auto& nested_type = as<list_type>(field_type).value_type();
  CHECK_EQUAL(nested_type, value_type);
}

} // namespace tenzir
