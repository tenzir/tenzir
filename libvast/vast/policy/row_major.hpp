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
#include <type_traits>

#include <caf/atom.hpp>

#include "vast/detail/column_iterator.hpp"

namespace vast::policy {

/// Configures a ::matrix_table_slice with a row-major memory layout.
template <class T>
struct row_major {
  // -- constants --------------------------------------------------------------

  static constexpr caf::atom_value class_id = caf::atom("TS_RowMaj");

  // -- member types -----------------------------------------------------------

  using row_iterator = T*;

  using const_row_iterator = const T*;

  using column_iterator = detail::column_iterator<T>;

  using const_column_iterator = detail::column_iterator<const T>;

  // -- factory functions ------------------------------------------------------

  /// @returns a random-access iterator to the first element in given column
  /// @pre `ptr` is at the first element of a contiguous memory block in
  ///      row-major order of size `rows * column`
  /// @post `result + rows` computes the past-the-end iterator
  template <class U>
  static detail::column_iterator<U>
  make_column_iterator(U* ptr, [[maybe_unused]] size_t rows, size_t columns,
                       size_t column) {
    static_assert(std::is_same_v<T, U> || std::is_same_v<const T, U>);
    return {ptr + column, static_cast<ptrdiff_t>(columns)};
  }

  // -- element access ---------------------------------------------------------

  /// Returns the array index for accessing the requested element.
  static constexpr size_t index_of(size_t, size_t columns, size_t row_position,
                                   size_t column_position) {
    return row_position * columns + column_position;
  }
};

} // namespace vast::policy
