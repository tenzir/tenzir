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

/// Counts false entries in `convertible` whose parent row is valid.
///
/// Null mask entries represent null source values, not failed conversions.
/// With no parent, every row is visible.
auto count_visible_conversion_failures(
  std::shared_ptr<arrow::BooleanArray> const& convertible,
  std::shared_ptr<arrow::Array> const& parent) -> int64_t;

} // namespace tenzir::plugins::iceberg::projection
