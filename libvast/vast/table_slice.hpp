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
#include <caf/ref_counted.hpp>

#include "vast/fwd.hpp"
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
  table_slice();

  /// Makes a copy of this slice.
  virtual table_slice* copy() const = 0;

  // -- persistence ------------------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice& x) {
    return f(x.layout_, x.offset_, x.rows_, x.columns_);
  }

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

  /// @returns the table layout.
  const record_type& layout() const noexcept {
    return layout_;
  }

  /// @returns an identifier for the implementing class.
  virtual caf::atom_value implementation_id() const noexcept = 0;

  /// @returns the layout for columns in range
  /// [first_column, first_column + num_columns).
  record_type layout(size_type first_column,
                     size_type num_columns = npos) const;

  /// @returns the number of rows in the slice.
  size_type rows() const noexcept {
    return rows_;
  }

  /// @returns the number of rows in the slice.
  size_type columns() const noexcept {
    return columns_;
  }

  /// @returns the offset in the ID space.
  id offset() const noexcept {
    return offset_;
  }

  /// Sets the offset in the ID space.
  void offset(id offset) noexcept {
    offset_ = offset;
  }

  /// Retrieves data by specifying 2D-coordinates via row and column.
  /// @param row The row offset.
  /// @param col The column offset.
  /// @pre `row < rows() && col < columns()`
  virtual data_view at(size_type row, size_type col) const = 0;

protected:
  // -- member variables -------------------------------------------------------

  id offset_;
  record_type layout_; // flattened
  size_type rows_;
  size_type columns_;
};

// -- free functions -----------------------------------------------------------

/// The factory function for default construction.
/// @relates table_slice
using table_slice_factory = table_slice_ptr (*)();

/// Registers a table slice factory for default construction.
/// @param id The unique implementation ID for the table slice
/// @param f The factory how to construct the table slice
/// @returns `true` iff the *f* was successfully associated with *id*.
/// @relates table_slice get_table_slice_factory
bool add_table_slice_factory(caf::atom_value id, table_slice_factory f);

/// Retrieves a table slice factory for default construction.
/// @relates table_slice add_table_slice_factory
table_slice_factory get_table_slice_factory(caf::atom_value id);

/// Default-constructs a table slice of a given type.
/// @param id The (registered) implementation ID of the slice.
/// @returns a handle holding an instance of type *impl* with given layout if
///          *impl* is a registered type in *sys*, otherwise `nullptr`.
/// @relates table_slice add_table_slice_factory get_table_slice_factory
table_slice_ptr make_table_slice(caf::atom_value id);

/// Constructs a table slice from a chunk. The beginning of the chunk must hold
/// the implementation ID of the concrete table slice. This function reads the
/// ID, default-constructs a new table slice with the given ID, and then calls
/// `table_slice::load` on the chunk.
/// @returns a table slice loaded from *chunk* or `nullptr` on failure.
/// @relates table_slice
table_slice_ptr make_table_slice(chunk_ptr chunk);

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
