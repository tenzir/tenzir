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
/// @relates table
class table_slice : public caf::ref_counted {
public:
  // -- member types -----------------------------------------------------------

  using size_type = uint64_t;

  static constexpr size_type npos = std::numeric_limits<size_type>::max();

  // -- constructors, destructors, and assignment operators --------------------

  ~table_slice();

  /// Constructs a table slice with a specific layout.
  /// @param layout The record describing the table columns.
  table_slice(record_type layout);

  // -- properties -------------------------------------------------------------

  /// @returns the table layout.
  inline const record_type& layout() const noexcept {
    return layout_;
  }

  /// @returns the layout for columns in range
  /// [first_column, first_column + num_columns).
  record_type layout(size_type first_column,
                     size_type num_columns = npos) const;

  /// @returns the content of a row wrapped into an value.
  caf::optional<value> row_to_value(size_type row, size_type first_column = 0u,
                                    size_type num_columns = npos) const;

  std::vector<value> rows_to_values(size_type first_row = 0u,
                                    size_type num_rows = npos,
                                    size_type first_column = 0u,
                                    size_type num_columns = npos) const;

  /// @returns the number of rows in the slice.
  inline size_type rows() const noexcept {
    return rows_;
  }

  /// @returns the number of rows in the slice.
  inline size_type columns() const noexcept {
    return columns_;
  }

  /// @returns the offset in the ID space.
  inline id offset() const noexcept {
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
  virtual caf::optional<data_view> at(size_type row, size_type col) const = 0;

protected:
  // -- member variables -------------------------------------------------------

  id offset_;
  record_type layout_; // flattened
  size_type rows_;
  size_type columns_;
};

/// @relates table_slice
bool operator==(const table_slice& x, const table_slice& y);

/// @relates table_slice
inline bool operator!=(const table_slice& x, const table_slice& y) {
  return !(x == y);
}

/// @relates table_slice
using table_slice_ptr = caf::intrusive_ptr<table_slice>;

/// @relates table_slice
using const_table_slice_ptr = caf::intrusive_ptr<const table_slice>;

} // namespace vast

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(vast::table_slice_ptr)

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::vector<vast::table_slice_ptr>)

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(vast::const_table_slice_ptr)

CAF_ALLOW_UNSAFE_MESSAGE_TYPE(std::vector<vast::const_table_slice_ptr>)
