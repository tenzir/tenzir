//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/table_slice.hpp"

#include <caf/meta/type_name.hpp>

namespace vast {

/// Additional state needed for the implementation of MessagePack-encoded table
/// slices that cannot easily be accessed from the underlying FlatBuffers table
/// directly.
template <class FlatBuffer>
struct msgpack_table_slice_state;

template <>
struct msgpack_table_slice_state<fbs::table_slice::msgpack::v0> {
  type layout;
  size_t columns;
};

template <>
struct msgpack_table_slice_state<fbs::table_slice::msgpack::v1> {
  type layout;
  size_t columns;
};

/// A table slice that stores elements encoded in
/// [MessagePack](https://msgpack.org) format. The implementation stores data
/// in row-major order.
template <class FlatBuffer>
class msgpack_table_slice final {
public:
  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs a MessagePack-encoded table slice from a FlatBuffers table.
  /// @param slice The encoding-specific FlatBuffers table.
  /// @param parent The surrounding chunk.
  /// @param batch A pre-existing record batch. Must always be nullptr.
  /// @param schema A VAST schema matching the record batch. Must always be none.
  msgpack_table_slice(const FlatBuffer& slice, const chunk_ptr& parent,
                      const std::shared_ptr<arrow::RecordBatch>& batch,
                      type schema) noexcept;

  /// Destroys a MessagePack-encoded table slice.
  ~msgpack_table_slice() noexcept;

  // -- properties -------------------------------------------------------------

  /// Whether the most recent version of the encoding is used.
  inline static constexpr bool is_latest_version
    = std::is_same_v<FlatBuffer, fbs::table_slice::msgpack::v1>;

  /// The encoding of the slice.
  inline static constexpr enum table_slice_encoding encoding
    = table_slice_encoding::msgpack;

  /// @returns The table layout.
  [[nodiscard]] const type& layout() const noexcept;

  /// @returns The number of rows in the slice.
  [[nodiscard]] table_slice::size_type rows() const noexcept;

  /// @returns The number of columns in the slice.
  [[nodiscard]] table_slice::size_type columns() const noexcept;

  /// @returns Whether the underlying buffer is serialized.
  [[nodiscard]] bool is_serialized() const noexcept;

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

private:
  // -- implementation details -------------------------------------------------

  /// A const-reference to the underlying FlatBuffers table.
  const FlatBuffer& slice_;

  /// Additional state needed for the implementation.
  msgpack_table_slice_state<FlatBuffer> state_;
};

// -- template machinery -------------------------------------------------------

/// Explicit deduction guide (not needed as of C++20).
template <class FlatBuffer>
msgpack_table_slice(const FlatBuffer&) -> msgpack_table_slice<FlatBuffer>;

/// Extern template declarations for all MessagePack encoding versions.
extern template class msgpack_table_slice<fbs::table_slice::msgpack::v0>;
extern template class msgpack_table_slice<fbs::table_slice::msgpack::v1>;

} // namespace vast
