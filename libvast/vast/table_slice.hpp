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

#include <cstddef>
#include <limits>
#include <vector>

#include <caf/allowed_unsafe_message_type.hpp>
#include <caf/fwd.hpp>
#include <caf/make_copy_on_write.hpp>
#include <caf/ref_counted.hpp>

#include "vast/fwd.hpp"
#include "vast/table_slice_header.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

namespace vast {

/// A horizontal partition of a table. A slice defines a tabular interface for
/// accessing homogenous data independent of the concrete carrier format.
class table_slice : public caf::ref_counted {
public:
  // -- member types -----------------------------------------------------------

  using size_type = uint64_t;

  static constexpr size_type npos = std::numeric_limits<size_type>::max();

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

  /// @returns the number of rows in the slice.
  size_type columns() const noexcept {
    return header_.layout.fields.size();
  }

  /// @returns the offset in the ID space.
  id offset() const noexcept {
    return header_.offset;
  }

  /// Sets the offset in the ID space.
  void offset(id offset) noexcept {
    header_.offset = offset;
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

/// Constructs table slices filled with random content for testing purposes.
/// @param num_slices The number of table slices to generate.
/// @param slice_size The number of rows per table slices.
/// @param layout The layout of the table slice.
/// @param offset The offset of the first table slize.
/// @param seed The seed value for initializing the random-number generator.
/// @returns a list of randomnly filled table slices or an error.
/// @relates table_slice
expected<std::vector<table_slice_ptr>>
make_random_table_slices(size_t num_slices, size_t slice_size,
                         record_type layout, id offset = 0, size_t seed = 0);

/// Selects all rows in `xs` with event IDs in `selection`. Cuts `xs` into
/// multiple slices if `selection` produces gaps.
/// @returns new table slices of the same implementation type as `xs` from
///          `selection`.
/// @pre `xs != nullptr`
std::vector<table_slice_ptr> select(const table_slice_ptr& xs,
                                    const ids& selection);

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
