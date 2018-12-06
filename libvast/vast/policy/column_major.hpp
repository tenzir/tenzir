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

#include <caf/atom.hpp>

#include "vast/detail/column_iterator.hpp"

namespace vast::policy {

/// Configures a ::matrix_table_slice with a column-major memory layout.
template <class T>
struct column_major {
  // -- constants --------------------------------------------------------------

  static constexpr caf::atom_value class_id = caf::atom("TS_ColMaj");

  // -- member types -----------------------------------------------------------

  using column_iterator = T*;

  using const_column_iterator = const T*;

  // -- factory functions ------------------------------------------------------

  /// @returns a random-access iterator to the first element in given column
  /// @pre `ptr` is at the first element of a contiguous memory block in
  ///      column-major order of size `rows * column`
  /// @post `result + columns` computes the past-the-end iterator
  template <class U>
  static U* make_column_iterator(U* ptr, size_t rows,
                                 [[maybe_unused]] size_t columns,
                                 size_t column) {
    return ptr + column * rows;
  }

  // -- element access ---------------------------------------------------------

  /// Returns the array index for accessing the requested element.
  static constexpr size_t index_of(size_t rows, size_t, size_t row_position,
                                   size_t column_position) {
    return column_position * rows + row_position;
  }
};

} // namespace vast::policy
