/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/fbs/table_slice.hpp"
#include "vast/fwd.hpp"
#include "vast/table_slice_header.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/fwd.hpp>
#include <caf/make_copy_on_write.hpp>
#include <caf/optional.hpp>
#include <caf/ref_counted.hpp>

#include <cstddef>
#include <limits>
#include <string_view>
#include <vector>

namespace vast {

/// A horizontal partition of a table. A slice defines a tabular interface for
/// accessing homogenous data independent of the concrete carrier format.
class table_slice : public caf::ref_counted {
public:
  // -- member types -----------------------------------------------------------

  using size_type = uint64_t;

  static constexpr size_type npos = std::numeric_limits<size_type>::max();

  /// Convenience helper for traversing a column.
  class column_view {
  public:
    column_view(const table_slice& slice, size_t column);

    /// @returns the data at given row.
    data_view operator[](size_t row) const;

    /// @returns the number of rows in the slice.
    size_t rows() const noexcept {
      return slice_.rows();
    }

    /// @returns the viewed table slice.
    const table_slice& slice() const noexcept {
      return slice_;
    }

    /// @returns the viewed column.
    size_t column() const noexcept {
      return column_;
    }

  private:
    const table_slice& slice_;
    size_t column_;
  };

  /// Convenience helper for traversing a row.
  class row_view {
  public:
    row_view(const table_slice& slice, size_t row);

    /// @returns the data at given column.
    data_view operator[](size_t column) const;

    /// @returns the number of columns in the slice.
    size_t columns() const noexcept {
      return slice_.columns();
    }

    /// @returns the viewed table slice.
    const table_slice& slice() const noexcept {
      return slice_;
    }

    /// @returns the viewed row.
    size_t row() const noexcept {
      return row_;
    }

  private:
    const table_slice& slice_;
    size_t row_;
  };

  // -- constructors, destructors, and assignment operators --------------------

  ~table_slice() override;

  table_slice(const table_slice&) = default;

  /// Default-constructs an empty table slice.
  table_slice() = default;

  /// Constructs a table slice from a header.
  /// @param header The header of the table slice.
  explicit table_slice(table_slice_header header = {});

  /// Makes a copy of this slice.
  virtual table_slice* copy() const = 0;

  // -- persistence ------------------------------------------------------------

  /// Saves the contents (excluding the layout!) of this slice to `sink`.
  virtual caf::error serialize(caf::serializer& sink) const = 0;

  /// Loads the contents for this slice from `source`.
  virtual caf::error deserialize(caf::deserializer& source) = 0;

  /// Loads a table slice from a chunk. Note that the beginning of the chunk
  /// data must point to the table slice data right after the implementation
  /// ID. The default implementation dispatches to `deserialize` with a
  /// `caf::binary_deserializer`.
  /// @param chunk The chunk to convert into a table slice.
  /// @returns An error if the operation fails and `none` otherwise.
  /// @pre `chunk != nullptr`
  virtual caf::error load(chunk_ptr chunk);

  // -- visitation -------------------------------------------------------------

  /// Appends all values in column `col` to `idx`.
  virtual void append_column_to_index(size_type col, value_index& idx) const;

  // -- properties -------------------------------------------------------------

  /// @returns the table slice header.
  const table_slice_header& header() const noexcept {
    return header_;
  }

  /// @returns the table layout.
  const record_type& layout() const noexcept {
    return header_.layout;
  }

  /// @returns an identifier for the implementing class.
  virtual caf::atom_value implementation_id() const noexcept = 0;

  /// @returns the layout for columns in range
  /// [first_column, first_column + num_columns).
  record_type layout(size_type first_column,
                     size_type num_columns = npos) const;

  /// @returns the number of rows in the slice.
  size_type rows() const noexcept {
    return header_.rows;
  }

  /// @returns a row view for the given `index`.
  /// @pre `row < rows()`
  row_view row(size_t index) const;

  /// @returns the number of rows in the slice.
  size_type columns() const noexcept {
    return header_.layout.fields.size();
  }

  /// @returns a column view for the given `index`.
  /// @pre `column < columns()`
  column_view column(size_t index) const;

  /// @returns a view for the column with given `name` on success, or `none` if
  ///          no column matches the `name`.
  caf::optional<column_view> column(std::string_view name) const;

  /// @returns the offset in the ID space.
  id offset() const noexcept {
    return header_.offset;
  }

  /// Sets the offset in the ID space.
  void offset(id offset) noexcept {
    header_.offset = offset;
  }

  /// @returns the name of a column.
  /// @param column The column offset.
  std::string_view column_name(size_t column) const noexcept {
    return header_.layout.fields[column].name;
  }

  /// Retrieves data by specifying 2D-coordinates via row and column.
  /// @param row The row offset.
  /// @param col The column offset.
  /// @pre `row < rows() && col < columns()`
  virtual data_view at(size_type row, size_type col) const = 0;

protected:
  // -- member variables -------------------------------------------------------

  table_slice_header header_;
};

// -- free functions -----------------------------------------------------------

/// Packs a table slice into a flatbuffer.
/// @param builder The builder to pack *x* into.
/// @param x The table slice to pack.
/// @returns The flatbuffer offset in *builder*.
caf::expected<flatbuffers::Offset<fbs::TableSliceBuffer>>
pack(flatbuffers::FlatBufferBuilder& builder, table_slice_ptr x);

/// Unpacks a table slice from a flatbuffer.
/// @param x The flatbuffer to unpack.
/// @param y The target to unpack *x* into.
/// @returns An error iff the operation fails.
caf::error unpack(const fbs::TableSlice& x, table_slice_ptr& y);

/// Constructs table slices filled with random content for testing purposes.
/// @param num_slices The number of table slices to generate.
/// @param slice_size The number of rows per table slices.
/// @param layout The layout of the table slice.
/// @param offset The offset of the first table slize.
/// @param seed The seed value for initializing the random-number generator.
/// @returns a list of randomnly filled table slices or an error.
/// @relates table_slice
caf::expected<std::vector<table_slice_ptr>>
make_random_table_slices(size_t num_slices, size_t slice_size,
                         record_type layout, id offset = 0, size_t seed = 0);

/// Selects all rows in `xs` with event IDs in `selection` and appends produced
/// table slices to `result`. Cuts `xs` into multiple slices if `selection`
/// produces gaps.
/// @param result The container for appending generated table slices.
/// @param xs The input table slice.
/// @param selection ID set for selecting events from `xs`.
/// @pre `xs != nullptr`
void select(std::vector<table_slice_ptr>& result, const table_slice_ptr& xs,
            const ids& selection);

/// Selects all rows in `xs` with event IDs in `selection`. Cuts `xs` into
/// multiple slices if `selection` produces gaps.
/// @param xs The input table slice.
/// @param selection ID set for selecting events from `xs`.
/// @returns new table slices of the same implementation type as `xs` from
///          `selection`.
/// @pre `xs != nullptr`
std::vector<table_slice_ptr> select(const table_slice_ptr& xs,
                                    const ids& selection);

/// Selects the first `num_rows` rows of `slice`.
/// @param slice The input table slice.
/// @param num_rows The number of rows to keep.
/// @returns `slice` if `slice->rows() <= num_rows`, otherwise creates a new
///          table slice of the first `num_rows` rows from `slice`.
/// @pre `slice != nullptr`
/// @pre `num_rows > 0`
table_slice_ptr truncate(const table_slice_ptr& slice, size_t num_rows);

/// Splits a table slice into two slices such that the first slice contains the
/// rows `[0, partition_point)` and the second slice contains the rows
/// `[partition_point, n)`, where `n = slice->rows()`.
/// @param slice The input table slice.
/// @param partition_point The index of the first row for the second slice.
/// @returns two new table slices if `0 < partition_point < slice->rows()`,
///          otherwise returns `slice` and a `nullptr`.
/// @pre `slice != nullptr`
std::pair<table_slice_ptr, table_slice_ptr> split(const table_slice_ptr& slice,
                                                  size_t partition_point);

/// @relates table_slice
bool operator==(const table_slice& x, const table_slice& y);

/// @relates table_slice
inline bool operator!=(const table_slice& x, const table_slice& y) {
  return !(x == y);
}

/// @relates table_slice
void intrusive_ptr_add_ref(const table_slice* ptr);

/// @relates table_slice
void intrusive_ptr_release(const table_slice* ptr);

/// @relates table_slice
table_slice* intrusive_cow_ptr_unshare(table_slice*&);

/// @relates table_slice
using table_slice_ptr = caf::intrusive_cow_ptr<table_slice>;

/// @relates table_slice
caf::error inspect(caf::serializer& sink, table_slice_ptr& ptr);

/// @relates table_slice
caf::error inspect(caf::deserializer& source, table_slice_ptr& ptr);

} // namespace vast
