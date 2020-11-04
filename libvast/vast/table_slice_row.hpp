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
#include "vast/view.hpp"

#include <caf/meta/type_name.hpp>

#include <cstdint>
#include <string_view>

namespace vast {

/// Convenience helper for traversing a row of a table slice.
/// @relates table_slice
class table_slice_row {
public:
  /// Defaulted constructors, destructors, and assignment operators.
  table_slice_row() noexcept;
  ~table_slice_row() noexcept;
  table_slice_row(const table_slice_row&) noexcept;
  table_slice_row& operator=(const table_slice_row&) noexcept;
  table_slice_row(table_slice_row&&) noexcept;
  table_slice_row& operator=(table_slice_row&&) noexcept;

  /// Construct a view on a row of a table slice.
  /// @param slice The slice to view.
  /// @param row The viewed row's index.
  /// @pre `row < slice->rows()`
  table_slice_row(table_slice_ptr slice, size_t row) noexcept;

  /// @returns the data at given column.
  /// @pre `column < size()`
  data_view operator[](size_t column) const;

  /// @returns the number of columns in the row.
  size_t size() const noexcept;

  /// @returns the viewed table slice.
  const table_slice_ptr& slice() const noexcept;

  /// @returns the viewed row's index.
  size_t index() const noexcept;

  /// Opt-in to CAF's type inspection API.
  template <class Inspector>
  friend auto inspect(Inspector& f, table_slice_row& x) ->
    typename Inspector::result_type {
    return f(caf::meta::type_name("vast.table_slice_row"), x.slice_, x.row_);
  }

private:
  table_slice_ptr slice_ = nullptr;
  size_t row_ = 0;
};

} // namespace vast
