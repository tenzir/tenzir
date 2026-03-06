//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/arrow_utils.hpp"
#include "tenzir/test/test.hpp"

#include <arrow/api.h>

namespace tenzir {
namespace {

auto make_int64_list_array(std::initializer_list<int64_t> values,
                           int64_t slice_offset, int64_t slice_length,
                           std::initializer_list<int32_t> offsets)
  -> std::shared_ptr<arrow::ListArray> {
  auto value_builder = arrow::Int64Builder{tenzir::arrow_memory_pool()};
  ::tenzir::check(value_builder.AppendValues(values));
  auto sliced_values = finish(value_builder)->Slice(slice_offset, slice_length);

  auto offset_builder = arrow::Int32Builder{tenzir::arrow_memory_pool()};
  ::tenzir::check(offset_builder.AppendValues(offsets));
  auto offset_array = std::shared_ptr<arrow::Int32Array>{};
  ::tenzir::check(offset_builder.Finish(&offset_array));

  return ::tenzir::check(arrow::ListArray::FromArrays(
    *offset_array, *sliced_values, arrow_memory_pool()));
}

TEST("append_array rebases invalid list offsets for sliced child arrays") {
  auto list = make_int64_list_array({10, 20, 30, 40, 50}, 2, 3, {2, 4, 5});
  REQUIRE(! list->ValidateFull().ok());

  auto ty = list_type{int64_type{}};
  auto builder = ty.make_arrow_builder(arrow_memory_pool());
  ::tenzir::check(append_array(*builder, ty, *list));
  auto result = finish(*builder);
  auto result_values
    = std::static_pointer_cast<arrow::Int64Array>(result->values());

  REQUIRE(result->ValidateFull().ok());
  REQUIRE_EQUAL(result->length(), int64_t{2});
  CHECK_EQUAL(result->value_offset(0), 0);
  CHECK_EQUAL(result->value_length(0), 2);
  CHECK_EQUAL(result->value_offset(1), 2);
  CHECK_EQUAL(result->value_length(1), 1);
  REQUIRE_EQUAL(result_values->length(), int64_t{3});
  CHECK_EQUAL(result_values->Value(0), int64_t{30});
  CHECK_EQUAL(result_values->Value(1), int64_t{40});
  CHECK_EQUAL(result_values->Value(2), int64_t{50});
}

TEST("append_array preserves valid list offsets for sliced child arrays") {
  auto list
    = make_int64_list_array({10, 20, 30, 40, 50, 60, 70}, 2, 5, {2, 3, 4});
  REQUIRE(list->ValidateFull().ok());

  auto ty = list_type{int64_type{}};
  auto builder = ty.make_arrow_builder(arrow_memory_pool());
  ::tenzir::check(append_array(*builder, ty, *list));
  auto result = finish(*builder);
  auto result_values
    = std::static_pointer_cast<arrow::Int64Array>(result->values());

  REQUIRE(result->ValidateFull().ok());
  REQUIRE_EQUAL(result->length(), int64_t{2});
  CHECK_EQUAL(result->value_offset(0), 0);
  CHECK_EQUAL(result->value_length(0), 1);
  CHECK_EQUAL(result->value_offset(1), 1);
  CHECK_EQUAL(result->value_length(1), 1);
  REQUIRE_EQUAL(result_values->length(), int64_t{2});
  CHECK_EQUAL(result_values->Value(0), int64_t{50});
  CHECK_EQUAL(result_values->Value(1), int64_t{60});
}

} // namespace
} // namespace tenzir
