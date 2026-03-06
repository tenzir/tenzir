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

TEST("append_array rebases list offsets for sliced child arrays") {
  auto value_builder = arrow::Int64Builder{tenzir::arrow_memory_pool()};
  ::tenzir::check(value_builder.AppendValues({10, 20, 30, 40, 50}));
  auto values = finish(value_builder);
  auto sliced_values = values->Slice(2, 3);

  auto offset_builder = arrow::Int32Builder{tenzir::arrow_memory_pool()};
  ::tenzir::check(offset_builder.Append(2));
  ::tenzir::check(offset_builder.Append(4));
  ::tenzir::check(offset_builder.Append(5));
  auto offsets = std::shared_ptr<arrow::Int32Array>{};
  ::tenzir::check(offset_builder.Finish(&offsets));

  auto list = ::tenzir::check(arrow::ListArray::FromArrays(
    *offsets, *sliced_values, arrow_memory_pool()));
  REQUIRE(! list->ValidateFull().ok());

  auto ty = list_type{int64_type{}};
  auto builder = ty.make_arrow_builder(arrow_memory_pool());
  ::tenzir::check(append_array(*builder, ty, *list));
  auto result = finish(*builder);
  auto result_values
    = std::static_pointer_cast<arrow::Int64Array>(result->values());

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

} // namespace
} // namespace tenzir
