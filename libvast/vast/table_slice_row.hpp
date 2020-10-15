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

// -- v1 includes --------------------------------------------------------------

#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"
#include "vast/view.hpp"

#include <caf/meta/type_name.hpp>

namespace vast {

namespace v1 {

class table_slice_row final {
public:
  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs an iterable view on a column of a table slice.
  /// @param slice The table slice to view.
  /// @param column The column index of the viewed slice.
  table_slice_row(table_slice slice, table_slice::size_type column) noexcept;

  /// Default-constructs an invalid table slice column.
  table_slice_row() noexcept;

  /// Copy-constructs a table slice column.
  table_slice_row(const table_slice_row& other) noexcept;
  table_slice_row& operator=(const table_slice_row& rhs) noexcept;

  /// Move-constructs a table slice column.
  table_slice_row(table_slice_row&& other) noexcept;
  table_slice_row& operator=(table_slice_row&& rhs) noexcept;

  /// Destroys a table slice column.
  ~table_slice_row() noexcept;

  // -- properties -------------------------------------------------------------

  /// @returns The index of the row in its slice.
  table_slice::size_type index() const noexcept;

  /// @returns The viewed slice.
  const table_slice& slice() const noexcept;

  /// @returns The number of columns in the row.
  table_slice::size_type size() const noexcept;

  /// @returns the data at given column.
  /// @pre `column < size()`
  data_view operator[](table_slice::size_type column) const;

  // -- type introspection -----------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice_row& row) {
    return f(caf::meta::type_name("table_slice_row"), row.slice_, row.row_);
  }

private:
  // -- implementation details -------------------------------------------------

  table_slice slice_ = {};
  table_slice::size_type row_ = 0;
};

} // namespace v1

} // namespace vast
