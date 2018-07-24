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

#include <cstdint>
#include <vector>

#include "vast/fwd.hpp"
#include "vast/type.hpp"
#include "vast/view.hpp"

namespace vast {

/// A dataset in tabular form. A table consists of [@ref table_slice](slices),
/// each of which have the same layout.
/// @relates table_slice
class table {
public:
  // -- member types -----------------------------------------------------------

  using size_type = uint64_t;

  using value_type = std::pair<id, const_table_slice_handle>;

  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs a table with a specific layout.
  /// @param layout The record describing the table columns.
  table(record_type layout);

  // -- properties ------------------------------------------------------------

  /// Adds a slice to the table.
  /// @returns A failure if the layout is not compatible with
  bool add(const_table_slice_handle slice);

  /// Retrieves the table layout.
  const record_type& layout() const noexcept {
    return layout_;
  }

  /// @returns the number of rows in the table.
  size_type rows() const {
    return slices_.size();
  }

  /// @returns the number of rows in the table.
  size_type columns() const {
    return columns_;
  }

  /// Retrieves data by specifying 2D-coordinates via row and column.
  /// @param row The row offset.
  /// @param col The column offset.
  /// @pre `row < rows() && col < columns()`
  caf::optional<data_view> at(size_type row, size_type col) const;

  /// @returns The slices in this table.
  const auto& slices() const {
    return slices_;
  }

private:
  record_type layout_;
  size_type columns_;
  std::vector<value_type> slices_;
};

} // namespace vast
