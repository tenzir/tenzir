//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/detail/projection.hpp"

#include <tenzir/detail/assert.hpp>

#include <arrow/array/array_nested.h>
#include <arrow/type.h>

#include <algorithm>

namespace tenzir::plugins::iceberg::projection {

namespace {

struct AlignmentBudget {
  uint64_t remaining;

  auto buffer(uint64_t count, uint64_t width) -> bool {
    if (width == 0 or count == 0) {
      return true;
    }
    if (count > remaining / width) {
      return false;
    }
    auto const bytes = count * width;
    auto const padding = (64 - bytes % 64) % 64;
    if (padding > remaining - bytes) {
      return false;
    }
    remaining -= bytes + padding;
    return true;
  }

  auto bitmap(uint64_t rows) -> bool {
    return buffer(rows / 8 + (rows % 8 != 0), 1);
  }

  auto offsets(uint64_t rows) -> bool {
    // Charging the final offset separately avoids overflowing rows + 1.
    return buffer(rows, 4) and buffer(1, 4);
  }
};

auto charge_null(arrow::DataType const& target, uint64_t rows,
                 AlignmentBudget& budget) -> bool {
  if (not budget.bitmap(rows)) {
    return false;
  }
  switch (target.id()) {
    case arrow::Type::STRUCT:
      for (auto const& field : target.fields()) {
        if (not charge_null(*field->type(), rows, budget)) {
          return false;
        }
      }
      return true;
    case arrow::Type::LIST:
      return budget.offsets(rows)
             and charge_null(*target.field(0)->type(), 0, budget);
    case arrow::Type::STRING:
    case arrow::Type::BINARY:
      return budget.offsets(rows);
    case arrow::Type::BOOL:
      return budget.bitmap(rows);
    case arrow::Type::NA:
      return true;
    default:
      return target.byte_width() > 0
             and budget.buffer(rows,
                               static_cast<uint64_t>(target.byte_width()));
  }
}

auto charge_alignment(arrow::Array const* source, arrow::DataType const& target,
                      uint64_t rows, AlignmentBudget& budget) -> bool {
  if (not source or source->type_id() == arrow::Type::NA) {
    return charge_null(target, rows, budget);
  }
  if (target.id() == arrow::Type::STRUCT
      and source->type_id() == arrow::Type::STRUCT) {
    auto const& record = static_cast<arrow::StructArray const&>(*source);
    if (record.null_count() > 0 and not budget.bitmap(rows)) {
      return false;
    }
    for (auto const& field : target.fields()) {
      auto child = record.GetFieldByName(field->name());
      if (not charge_alignment(child.get(), *field->type(), rows, budget)) {
        return false;
      }
    }
    return true;
  }
  if (target.id() == arrow::Type::LIST
      and source->type_id() == arrow::Type::LIST) {
    auto const& list = static_cast<arrow::ListArray const&>(*source);
    auto const begin = list.value_offset(0);
    auto const end = list.value_offset(list.length());
    auto const exact = list.offset() == 0 and list.null_count() == 0
                       and begin == 0 and end == list.values()->length();
    if (not exact and not budget.offsets(rows)) {
      return false;
    }
    if (list.null_count() > 0) {
      if (not budget.bitmap(rows)) {
        return false;
      }
      // Flatten may copy the child payload when null rows span values.
      // Close these buffers before alignment rather than budget an unbounded
      // payload copy in addition to the new null-filled fields.
      for (auto index = int64_t{0}; index < list.length(); ++index) {
        if (list.IsNull(index) and list.value_length(index) > 0) {
          return false;
        }
      }
    }
    auto values = list.values()->Slice(begin, end - begin);
    return charge_alignment(values.get(), *target.field(0)->type(),
                            static_cast<uint64_t>(values->length()), budget);
  }
  return source->type()->Equals(target);
}

} // namespace

auto fits_alignment_budget(arrow::Array const& source,
                           arrow::DataType const& target, uint64_t bytes)
  -> bool {
  if (source.type()->Equals(target)) {
    return true;
  }
  for (auto offset = int64_t{0}; offset < source.length();
       offset += std::min(alignment_batch_rows, source.length() - offset)) {
    auto chunk = source.Slice(offset, std::min(alignment_batch_rows,
                                               source.length() - offset));
    auto budget = AlignmentBudget{bytes};
    if (not charge_alignment(chunk.get(), target,
                             static_cast<uint64_t>(chunk->length()), budget)) {
      return false;
    }
  }
  return true;
}

auto count_visible_conversion_failures(
  std::shared_ptr<arrow::BooleanArray> const& convertible,
  std::shared_ptr<arrow::Array> const& parent) -> int64_t {
  TENZIR_ASSERT(not parent or parent->length() == convertible->length());
  if (not parent or parent->null_count() == 0) {
    return convertible->false_count();
  }
  auto failures = int64_t{0};
  for (auto index = int64_t{0}; index < convertible->length(); ++index) {
    if (parent->IsValid(index) and convertible->IsValid(index)
        and not convertible->Value(index)) {
      ++failures;
    }
  }
  return failures;
}

} // namespace tenzir::plugins::iceberg::projection
