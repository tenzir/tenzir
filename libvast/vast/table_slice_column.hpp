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

#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"
#include "vast/view.hpp"

#include <caf/meta/type_name.hpp>

#include <string>
#include <string_view>

namespace vast {

class table_slice_column final {
public:
  // -- constructors, destructors, and assignment operators --------------------

  /// Constructs an iterable view on a column of a table slice.
  /// @param slice The table slice to view.
  /// @param column The column index of the viewed slice.
  table_slice_column(table_slice slice, table_slice::size_type column) noexcept;

  /// Constructs an iterable view on a column of a table slice.
  /// @param slice The table slice to view.
  /// @param name The column name of the viewed slice.
  /// @note The owned slice is set to invalid if the name does not refer to a
  /// valid column. Use `x.slice().encoding() != table_slice_encoding::invalid`
  /// to check for failure.
  table_slice_column(table_slice slice, std::string_view name) noexcept;

  /// Default-constructs an invalid table slice column.
  table_slice_column() noexcept;

  /// Copy-constructs a table slice column.
  table_slice_column(const table_slice_column& other) noexcept;
  table_slice_column& operator=(const table_slice_column& rhs) noexcept;

  /// Move-constructs a table slice column.
  table_slice_column(table_slice_column&& other) noexcept;
  table_slice_column& operator=(table_slice_column&& rhs) noexcept;

  /// Destroys a table slice column.
  ~table_slice_column() noexcept;

  // -- properties -------------------------------------------------------------

  /// @returns The index of the column in its slice.
  table_slice::size_type index() const noexcept;

  /// @returns The viewed slice.
  const table_slice& slice() const noexcept;

  /// @returns The number of rows in the column.
  table_slice::size_type size() const noexcept;

  /// @returns The data at given row.
  /// @pre `row < size()`
  data_view operator[](table_slice::size_type row) const;

  /// @returns The name of the column.
  std::string name() const noexcept;

  // -- type introspection -----------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice_column& column) {
    return f(caf::meta::type_name("table_slice_column"), column.slice_,
             column.column_);
  }

private:
  // -- implementation details -------------------------------------------------

  table_slice slice_ = {};
  table_slice::size_type column_ = 0;
};

} // namespace vast
