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

#include "vast/table_slice_row.hpp"

#include "vast/table_slice.hpp"

namespace vast {

namespace v1 {

// -- constructors, destructors, and assignment operators ----------------------

table_slice_row::table_slice_row(table_slice slice,
                                 table_slice::size_type row) noexcept
  : slice_{slice}, row_{row} {
  // nop
}

table_slice_row::table_slice_row() noexcept = default;

table_slice_row::table_slice_row(const table_slice_row& other) noexcept
  : slice_{other.slice_}, row_{other.row_} {
  // nop
}

table_slice_row&
table_slice_row::operator=(const table_slice_row& rhs) noexcept {
  slice_ = rhs.slice_;
  row_ = rhs.row_;
  return *this;
}

table_slice_row::table_slice_row(table_slice_row&& other) noexcept
  : slice_{std::exchange(other.slice_, {})},
    row_{std::exchange(other.row_, {})} {
  // nop
}

table_slice_row& table_slice_row::operator=(table_slice_row&& rhs) noexcept {
  slice_ = std::exchange(rhs.slice_, {});
  row_ = std::exchange(rhs.row_, {});
  return *this;
}

table_slice_row::~table_slice_row() noexcept = default;

// -- properties ---------------------------------------------------------------

table_slice::size_type table_slice_row::index() const noexcept {
  return row_;
}

const table_slice& table_slice_row::slice() const noexcept {
  return slice_;
}

table_slice::size_type table_slice_row::size() const noexcept {
  return slice_.columns();
}

data_view table_slice_row::operator[](table_slice::size_type column) const {
  VAST_ASSERT(column < size());
  return slice_.at(row_, column);
}

} // namespace v1

} // namespace vast
