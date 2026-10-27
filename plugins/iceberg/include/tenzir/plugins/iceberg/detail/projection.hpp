//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <arrow/array/array_base.h>
#include <arrow/array/array_primitive.h>

#include <cstdint>
#include <memory>

namespace tenzir::plugins::iceberg::projection {

inline constexpr auto alignment_batch_rows = int64_t{4096};

/// Checks a conservative budget for null-fill buffers and regenerated nested
/// validity/offset buffers in each alignment chunk, without allocating them.
/// Existing source payload and Parquet encoder memory are outside this budget.
/// A single list can reference many more values than its top-level row count.
/// Alignment that may copy payload hidden beneath null list rows is declined.
auto fits_alignment_budget(arrow::Array const& source,
                           arrow::DataType const& target, uint64_t bytes)
  -> bool;

/// Counts false entries in `convertible` whose parent row is valid.
///
/// Null mask entries represent null source values, not failed conversions.
/// With no parent, every row is visible.
auto count_visible_conversion_failures(
  std::shared_ptr<arrow::BooleanArray> const& convertible,
  std::shared_ptr<arrow::Array> const& parent) -> int64_t;

} // namespace tenzir::plugins::iceberg::projection
