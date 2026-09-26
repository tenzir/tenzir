//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <arrow/result.h>
#include <arrow/type_fwd.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <vector>

namespace parquet {
class FileMetaData;
namespace arrow {
struct SchemaManifest;
} // namespace arrow
} // namespace parquet

namespace tenzir::plugins::parquet {

/// Formats a decoded array the way the reader does before importing it.
using FormatArray = std::function<arrow::Result<std::shared_ptr<arrow::Array>>(
  std::shared_ptr<arrow::Array>)>;

/// The selected leaf columns that the statistics of the row groups to read
/// prove constant, so that the reader need not decode them.
///
/// A column qualifies if it holds the same valid value in all of the row
/// groups, going by exact bounds, or nothing but nulls. Its value is what the
/// import makes of the decoded column, which follows the Arrow type that the
/// reader restores for it. Columns whose import could fail, floats with values,
/// whose statistics leave out NaNs, and leaves inside lists and maps never
/// qualify.
class ConstantColumns {
public:
  /// Plans the constant columns among `columns` of `row_groups`, both sorted
  /// in ascending order. Without a `manifest`, none of them are.
  static auto make(::parquet::FileMetaData const& metadata,
                   ::parquet::arrow::SchemaManifest const* manifest,
                   std::span<int const> row_groups,
                   std::span<int const> columns, FormatArray const& format)
    -> ConstantColumns;

  /// Adds the constant columns to a batch of the decoded ones, in schema
  /// order, also within records. The constants import without per-row work.
  auto complete(std::shared_ptr<arrow::RecordBatch> batch)
    -> arrow::Result<std::shared_ptr<arrow::RecordBatch>>;

  /// The columns that remain to be decoded, in ascending order.
  auto decoded() const -> std::vector<int> const&;

private:
  /// A selected field and where its values come from.
  struct Node {
    enum class Kind : uint8_t {
      /// The reader decodes the field.
      decoded,
      /// A leaf of a single value, which repeats `value`.
      constant,
      /// A leaf that is null throughout.
      null,
      /// A record, whose fields are the `children`.
      record,
    };

    std::shared_ptr<arrow::Field> field;
    Kind kind = Kind::decoded;
    /// The value of a constant, as a single-row array.
    std::shared_ptr<arrow::ArrayData> value;
    /// The leaf columns of a constant or null field, which need no decoding.
    std::vector<int> columns;
    /// The selected fields of a record, in schema order.
    std::vector<Node> children;
    /// Whether the decoded batches contain the field.
    bool decoded = true;
    /// Whether a leaf of the field comes from the statistics.
    bool constant = false;
    /// The array of the field if it is not decoded, for the last length.
    std::shared_ptr<arrow::ArrayData> cache;
  };

  class Planner;

  /// The array of a field that the batches do not contain.
  auto repeat(Node& node, int64_t length)
    -> arrow::Result<std::shared_ptr<arrow::ArrayData>>;

  /// Adds the constant fields to a decoded record.
  auto rebuild(Node& node, std::shared_ptr<arrow::ArrayData> data)
    -> arrow::Result<std::shared_ptr<arrow::ArrayData>>;

  std::vector<Node> fields_;
  std::vector<int> decoded_;
  bool any_ = false;
  /// Zeros, the indices that make every row of a dictionary its only value.
  std::shared_ptr<arrow::Buffer> zeros_;
};

} // namespace tenzir::plugins::parquet
