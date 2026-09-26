//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/option.hpp>
#include <tenzir/variant.hpp>

#include <arrow/type_fwd.h>

#include <cstdint>
#include <memory>
#include <string_view>

namespace parquet {
class ColumnDescriptor;
class Statistics;
} // namespace parquet

namespace tenzir::plugins::parquet {

/// A value of a leaf column as Parquet stores it. Byte arrays and fixed-length
/// byte arrays are their bytes.
using PhysicalValue
  = variant<bool, int32_t, int64_t, float, double, std::string_view>;

/// The minimum or, if `upper`, the maximum that the statistics of a column
/// chunk record, if any. Byte arrays refer to the memory of `stats`.
auto statistics_bound(::parquet::Statistics const& stats, bool upper)
  -> Option<PhysicalValue>;

/// A single-row array of what the reader decodes a value of `column` into.
///
/// `restored` is the Arrow type that the reader restores for the column, which
/// may differ from what the Parquet types say. Arrow writes durations as plain
/// `INT64`, for example, and restores them from the Arrow schema that it
/// embeds. Dictionaries and enumerations resolve to their values, so that the
/// array imports like a row of the column.
///
/// Returns `None` for types whose values the reader decodes differently, and
/// for values that the reader cannot represent in the restored type.
auto decode_value(::parquet::ColumnDescriptor const& column,
                  std::shared_ptr<::arrow::DataType> const& restored,
                  PhysicalValue const& value)
  -> Option<std::shared_ptr<::arrow::Array>>;

} // namespace tenzir::plugins::parquet
