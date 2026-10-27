//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/detail/projection.hpp"

#include "tenzir/option.hpp"

#include <tenzir/arrow_utils.hpp>
#include <tenzir/test/test.hpp>

#include <arrow/array/array_nested.h>
#include <arrow/array/util.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>

#include <initializer_list>
#include <limits>
#include <string>

namespace tenzir::plugins::iceberg {

namespace {

auto make_booleans(std::initializer_list<Option<bool>> values)
  -> std::shared_ptr<arrow::BooleanArray> {
  auto builder = arrow::BooleanBuilder{};
  for (auto value : values) {
    if (value) {
      check(builder.Append(*value));
    } else {
      check(builder.AppendNull());
    }
  }
  return std::static_pointer_cast<arrow::BooleanArray>(check(builder.Finish()));
}

TEST("conversion failures beneath null parents are hidden") {
  auto convertible = make_booleans({false, true});
  auto parent = make_booleans({None{}, true});
  CHECK(projection::count_visible_conversion_failures(convertible, parent)
        == 0);
}

TEST("conversion failures beneath valid parents remain visible") {
  auto convertible = make_booleans({false, false, None{}});
  auto parent = make_booleans({None{}, true, true});
  CHECK(projection::count_visible_conversion_failures(convertible, parent)
        == 1);
  CHECK(projection::count_visible_conversion_failures(convertible, nullptr)
        == 2);
}

auto record_values(int64_t rows) -> std::shared_ptr<arrow::StructArray> {
  return check(arrow::StructArray::Make(
    {check(arrow::MakeArrayOfNull(arrow::int64(), rows))},
    {arrow::field("id", arrow::int64())}));
}

auto wider_record(int fields) -> std::shared_ptr<arrow::DataType> {
  auto children = arrow::FieldVector{arrow::field("id", arrow::int64())};
  for (auto i = 0; i < fields; ++i) {
    children.push_back(arrow::field(std::to_string(i), arrow::int64()));
  }
  return arrow::struct_(std::move(children));
}

auto list_values(std::initializer_list<int32_t> offsets,
                 std::shared_ptr<arrow::Array> values)
  -> std::shared_ptr<arrow::ListArray> {
  auto builder = arrow::Int32Builder{};
  check(builder.AppendValues(offsets));
  return check(arrow::ListArray::FromArrays(*check(builder.Finish()), *values));
}

TEST("alignment budgets apply per top-level chunk") {
  auto source = record_values(8193);
  auto target = wider_record(64);
  CHECK(projection::fits_alignment_budget(*source, *target, 4 * 1024 * 1024));
  CHECK(not projection::fits_alignment_budget(*source, *target, 1024 * 1024));
  CHECK(projection::fits_alignment_budget(*source, *source->type(), 0));
}

TEST("alignment budgets count values inside one list row") {
  auto source = list_values({0, 8192}, record_values(8192));
  auto target = arrow::list(wider_record(64));
  CHECK(not projection::fits_alignment_budget(*source, *target, 1024 * 1024));
  CHECK(projection::fits_alignment_budget(*source, *target, 8 * 1024 * 1024));
  auto nested = list_values({0, 1}, source);
  auto nested_target = arrow::list(target);
  CHECK(not projection::fits_alignment_budget(*nested, *nested_target,
                                              1024 * 1024));
}

TEST("alignment budgets follow sliced list offsets") {
  auto source = list_values({0, 1, 8193}, record_values(8193));
  auto target = arrow::list(wider_record(64));
  CHECK(projection::fits_alignment_budget(*source->Slice(0, 1), *target,
                                          64 * 1024));
  CHECK(not projection::fits_alignment_budget(*source->Slice(1, 1), *target,
                                              1024 * 1024));
}

TEST("alignment declines list flattening that copies existing payload") {
  auto builder = arrow::Int64Builder{};
  tenzir::check(builder.AppendValues({1, 2, 3}));
  auto values = tenzir::check(builder.Finish());
  auto offsets = list_values({0, 1, 2, 3}, values);
  auto validity = make_booleans({true, None{}, true});
  auto list
    = std::make_shared<arrow::ListArray>(offsets->type(), 3,
                                         offsets->data()->buffers[1], values,
                                         validity->null_bitmap(), 1);
  // The null middle row hides a value, so Flatten copies the two live spans.
  auto pool = arrow::ProxyMemoryPool{arrow::default_memory_pool()};
  auto flat = tenzir::check(list->Flatten(&pool));
  CHECK(flat->length() == 2);
  CHECK(pool.total_bytes_allocated() > 0);
  auto source = tenzir::check(
    arrow::StructArray::Make({list}, {arrow::field("items", list->type())}));
  auto target = arrow::struct_({arrow::field("items", list->type()),
                                arrow::field("added", arrow::int64())});
  CHECK(not projection::fits_alignment_budget(*source, *target, 1024));
  CHECK(projection::fits_alignment_budget(*source, *source->type(), 0));
  // A slice containing only the first live list does not need a payload copy.
  CHECK(projection::fits_alignment_budget(*source->Slice(0, 1), *target, 1024));
  auto nested = list_values({0, 3}, list);
  auto nested_source = tenzir::check(arrow::StructArray::Make(
    {nested}, {arrow::field("items", nested->type())}));
  auto nested_target = arrow::struct_({arrow::field("items", nested->type()),
                                       arrow::field("added", arrow::int64())});
  CHECK(not projection::fits_alignment_budget(*nested_source, *nested_target,
                                              1024));
}

TEST("alignment accepts null lists backed by empty spans") {
  auto values = record_values(2);
  auto offsets = list_values({0, 1, 1, 2}, values);
  auto validity = make_booleans({true, None{}, true});
  auto list
    = std::make_shared<arrow::ListArray>(offsets->type(), 3,
                                         offsets->data()->buffers[1], values,
                                         validity->null_bitmap(), 1);
  CHECK(projection::fits_alignment_budget(*list, *arrow::list(wider_record(1)),
                                          1024));
}

TEST("alignment budgets include allocation padding and reject huge null "
     "fills") {
  auto source = arrow::NullArray{1};
  CHECK(projection::fits_alignment_budget(source, *arrow::int64(), 128));
  CHECK(not projection::fits_alignment_budget(source, *arrow::int64(), 127));
  auto const huge = std::numeric_limits<int32_t>::max();
  auto list = list_values({0, huge}, std::make_shared<arrow::NullArray>(huge));
  CHECK(not projection::fits_alignment_budget(
    *list, *arrow::list(arrow::int64()), 64 * 1024 * 1024));
}

} // namespace

} // namespace tenzir::plugins::iceberg
