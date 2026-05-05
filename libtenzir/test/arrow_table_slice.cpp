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
#include "tenzir/test/test.hpp"

#include <arrow/api.h>
#include <arrow/util/bit_util.h>

#include <cstring>

namespace tenzir {

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
    = record_batch_from_struct_array(arrow::schema(fields), struct_array);
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

TEST("record batch from struct array preserves offsets") {
  auto int_builder = arrow::Int64Builder{arrow_memory_pool()};
  tenzir::check(int_builder.Append(int64_t{42}));
  tenzir::check(int_builder.Append(int64_t{1337}));
  auto ints = finish(int_builder);
  auto string_builder = arrow::StringBuilder{arrow_memory_pool()};
  tenzir::check(string_builder.Append("skip"));
  tenzir::check(string_builder.Append("keep"));
  auto strings = finish(string_builder);
  auto fields = arrow::FieldVector{
    arrow::field("a", arrow::int64()),
    arrow::field("b", arrow::utf8()),
  };
  auto struct_array
    = make_struct_array(2, nullptr, fields, arrow::ArrayVector{ints, strings});
  auto sliced = std::dynamic_pointer_cast<arrow::StructArray>(
    struct_array->Slice(int64_t{1}, int64_t{1}));
  REQUIRE(sliced);
  auto batch = record_batch_from_struct_array(arrow::schema(fields), sliced);
  REQUIRE(batch);
  auto a
    = std::dynamic_pointer_cast<arrow::Int64Array>(batch->GetColumnByName("a"));
  auto b = std::dynamic_pointer_cast<arrow::StringArray>(
    batch->GetColumnByName("b"));
  REQUIRE(a);
  REQUIRE(b);
  CHECK_EQUAL(a->length(), int64_t{1});
  CHECK_EQUAL(b->length(), int64_t{1});
  CHECK(not a->IsNull(0));
  CHECK(not b->IsNull(0));
  CHECK_EQUAL(a->Value(0), int64_t{1337});
  CHECK_EQUAL(b->GetString(0), "keep");
}

TEST("transform columns preserves offsets and row nulls") {
  auto int_builder = arrow::Int64Builder{arrow_memory_pool()};
  tenzir::check(int_builder.Append(int64_t{42}));
  tenzir::check(int_builder.Append(int64_t{1337}));
  auto ints = finish(int_builder);
  auto string_builder = arrow::StringBuilder{arrow_memory_pool()};
  tenzir::check(string_builder.Append("leak"));
  tenzir::check(string_builder.Append("keep"));
  auto strings = finish(string_builder);
  auto bitmap = tenzir::check(arrow::AllocateBitmap(2, arrow_memory_pool()));
  std::memset(bitmap->mutable_data(), 0, bitmap->size());
  arrow::bit_util::SetBit(bitmap->mutable_data(), 1);
  auto fields = arrow::FieldVector{
    arrow::field("a", arrow::int64()),
    arrow::field("b", arrow::utf8()),
  };
  auto schema
    = type{"test", record_type{{"a", int64_type{}}, {"b", string_type{}}}};
  auto struct_array = make_struct_array(2, bitmap, fields, {ints, strings});
  auto batch
    = record_batch_from_struct_array(schema.to_arrow_schema(), struct_array);
  auto slice = table_slice{batch, schema};
  auto sliced = subslice(slice, 1, 2);
  auto transformed = transform_columns(
    sliced, std::vector<indexed_transformation>{{
              .index = {0},
              .fun =
                [](struct record_type::field field,
                   std::shared_ptr<arrow::Array> array) {
                  return indexed_transformation::result_type{
                    {std::move(field), std::move(array)}};
                },
            }});
  auto transformed_batch = to_record_batch(transformed);
  REQUIRE(transformed_batch);
  auto a = std::dynamic_pointer_cast<arrow::Int64Array>(
    transformed_batch->GetColumnByName("a"));
  auto b = std::dynamic_pointer_cast<arrow::StringArray>(
    transformed_batch->GetColumnByName("b"));
  REQUIRE(a);
  REQUIRE(b);
  CHECK_EQUAL(a->length(), int64_t{1});
  CHECK_EQUAL(b->length(), int64_t{1});
  CHECK(not a->IsNull(0));
  CHECK(not b->IsNull(0));
  CHECK_EQUAL(a->Value(0), int64_t{1337});
  CHECK_EQUAL(b->GetString(0), "keep");
}

} // namespace tenzir
