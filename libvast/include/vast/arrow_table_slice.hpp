//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/table_slice.hpp"

#include <arrow/type_fwd.h>
#include <caf/meta/type_name.hpp>

#include <memory>

namespace vast {

/// Additional state needed for the implementation of Arrow-encoded table slices
/// that cannot easily be accessed from the underlying FlatBuffers table
/// directly.
template <class FlatBuffer>
struct arrow_table_slice_state;

template <>
struct arrow_table_slice_state<fbs::table_slice::arrow::v0> {
  /// The deserialized table layout.
  type layout;

  /// The deserialized Arrow Record Batch.
  std::shared_ptr<arrow::RecordBatch> record_batch;
};

template <>
struct arrow_table_slice_state<fbs::table_slice::arrow::v1> {
  /// The deserialized table layout.
  type layout;

  /// The deserialized Arrow Record Batch.
  std::shared_ptr<arrow::RecordBatch> record_batch;
};

template <>
struct arrow_table_slice_state<fbs::table_slice::arrow::v2> {
  /// The deserialized table layout.
  type layout;

  /// The deserialized Arrow Record Batch.
  std::shared_ptr<arrow::RecordBatch> record_batch;

  /// Mapping from column offset to nested Arrow array
  arrow::ArrayVector flat_columns;
};

/// A table slice that stores elements encoded in the [Arrow](https://arrow.org)
/// format. The implementation stores data in column-major order.
template <class FlatBuffer>
class arrow_table_slice final {
public:
  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs an Arrow-encoded table slice from a FlatBuffers table.
  /// @param slice The encoding-specific FlatBuffers table.
  /// @param parent The surrounding chunk.
  arrow_table_slice(const FlatBuffer& slice, const chunk_ptr& parent) noexcept;

  /// Destroys a Arrow-encoded table slice.
  ~arrow_table_slice() noexcept;

  // -- properties -------------------------------------------------------------

  /// Whether the most recent version of the encoding is used.
  inline static constexpr bool is_latest_version
    = std::is_same_v<FlatBuffer, fbs::table_slice::arrow::v2>;

  /// The encoding of the slice.
  inline static constexpr enum table_slice_encoding encoding
    = table_slice_encoding::arrow;

  /// @returns The table layout.
  [[nodiscard]] const type& layout() const noexcept;

  /// @returns The number of rows in the slice.
  [[nodiscard]] table_slice::size_type rows() const noexcept;

  /// @returns The number of columns in the slice.
  [[nodiscard]] table_slice::size_type columns() const noexcept;

  // -- data access ------------------------------------------------------------

  /// Appends all values in column `column` to `index`.
  /// @param `offset` The offset of the table slice in its ID space.
  /// @param `column` The index of the column to append.
  /// @param `index` the value index to append to.
  void append_column_to_index(id offset, table_slice::size_type column,
                              value_index& index) const;

  /// Retrieves data by specifying 2D-coordinates via row and column.
  /// @param row The row offset.
  /// @param column The column offset.
  /// @pre `row < rows() && column < columns()`
  [[nodiscard]] data_view
  at(table_slice::size_type row, table_slice::size_type column) const;

  /// Retrieves data by specifying 2D-coordinates via row and column.
  /// @param row The row offset.
  /// @param column The column offset.
  /// @param t The type of the value to be retrieved.
  /// @pre `row < rows() && column < columns()`
  [[nodiscard]] data_view
  at(table_slice::size_type row, table_slice::size_type column,
     const type& t) const;

  /// @returns The import timestamp.
  [[nodiscard]] time import_time() const noexcept;

  /// Sets the import timestamp.
  void import_time(time import_time) noexcept;

  /// @returns A shared pointer to the underlying Arrow Record Batch.
  [[nodiscard]] std::shared_ptr<arrow::RecordBatch>
  record_batch() const noexcept;

private:
  // -- implementation details -------------------------------------------------

  /// A const-reference to the underlying FlatBuffers table.
  const FlatBuffer& slice_;

  /// Additional state needed for the implementation.
  arrow_table_slice_state<FlatBuffer> state_;
};

// -- utility functions -------------------------------------------------------

/// Access a VAST data view for a given row in an Array Array.
data_view value_at(const type& type, const std::same_as<arrow::Array> auto& arr,
                   int64_t row) noexcept;

/// Access VAST data views for all elements of an Arrow Array.
auto values(const type& type,
            const std::same_as<arrow::Array> auto& array) noexcept
  -> detail::generator<data_view>;

struct indexed_transformation {
  using function_type = std::function<std::vector<
    std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>>(
    struct record_type::field, std::shared_ptr<arrow::Array>)>;

  offset index;      ///< The index of the field to transform.
  function_type fun; /// The transformation function to apply.

  friend auto operator<=>(const indexed_transformation& lhs,
                          const indexed_transformation& rhs) noexcept {
    return lhs.index <=> rhs.index;
  }
};

/// Applies a list of transformations to both a VAST layout and an Arrow record
/// batch.
/// @pre VAST layout and Arrow schema must match.
/// @pre Transformations must be sorted by index.
/// @pre Transformation indices must not be a subset of the following
/// transformation's index.
std::pair<type, std::shared_ptr<arrow::RecordBatch>>
transform(type layout, const std::shared_ptr<arrow::RecordBatch>& batch,
          const std::vector<indexed_transformation>& transformations) noexcept;

/// Removed all unspecified columns from both a VAST layout and an Arrow record
/// batch.
/// @pre VAST layout and Arrow schema must match.
/// @pre Indices must be sorted.
/// @pre Indices must not be a subset of the following index.
std::pair<type, std::shared_ptr<arrow::RecordBatch>>
project(type layout, const std::shared_ptr<arrow::RecordBatch>& batch,
        const std::vector<offset>& indices) noexcept;

// -- template machinery -------------------------------------------------------

/// Explicit deduction guide (not needed as of C++20).
template <class FlatBuffer>
arrow_table_slice(const FlatBuffer&) -> arrow_table_slice<FlatBuffer>;

/// Extern template declarations for all Arrow encoding versions.
extern template class arrow_table_slice<fbs::table_slice::arrow::v0>;
extern template class arrow_table_slice<fbs::table_slice::arrow::v1>;
extern template class arrow_table_slice<fbs::table_slice::arrow::v2>;

// -- utility functions --------------------------------------------------------

/// Converts legacy (v0/v1) table slice to the new representation, re-using
/// existing Arrow arrays to make this a cheap operation.
/// @note subnet structure changed in a way that array is built from scratch.
table_slice convert_legacy_table_slice(const table_slice& legacy);

} // namespace vast
