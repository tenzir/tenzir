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

#include "vast/table_slice_column.hpp"

#include "vast/table_slice.hpp"

namespace vast {

namespace v1 {

// -- constructors, destructors, and assignment operators ----------------------

table_slice_column::table_slice_column(table_slice slice,
                                       table_slice::size_type column) noexcept
  : slice_{slice}, column_{column} {
  // nop
}

table_slice_column::table_slice_column() noexcept = default;

table_slice_column::table_slice_column(const table_slice_column& other) noexcept
  : slice_{other.slice_}, column_{other.column_} {
  // nop
}

table_slice_column&
table_slice_column::operator=(const table_slice_column& rhs) noexcept {
  slice_ = rhs.slice_;
  column_ = rhs.column_;
  return *this;
}

table_slice_column::table_slice_column(table_slice_column&& other) noexcept
  : slice_{std::exchange(other.slice_, {})},
    column_{std::exchange(other.column_, {})} {
  // nop
}

table_slice_column&
table_slice_column::operator=(table_slice_column&& rhs) noexcept {
  slice_ = std::exchange(rhs.slice_, {});
  column_ = std::exchange(rhs.column_, {});
  return *this;
}

table_slice_column::~table_slice_column() noexcept = default;

// -- properties ---------------------------------------------------------------

table_slice::size_type table_slice_column::index() const noexcept {
  return column_;
}

const table_slice& table_slice_column::slice() const noexcept {
  return slice_;
}

table_slice::size_type table_slice_column::size() const noexcept {
  return slice_.rows();
}

data_view table_slice_column::operator[](table_slice::size_type row) const {
  VAST_ASSERT(row < size());
  return slice_.at(row, column_);
}

std::string table_slice_column::name() const noexcept {
  return slice_.layout().fields[column_].name;
}

} // namespace v1

} // namespace vast
