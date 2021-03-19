//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/table_slice_row.hpp"

#include "vast/detail/assert.hpp"
#include "vast/table_slice.hpp"

namespace vast {

table_slice_row::table_slice_row() noexcept = default;

table_slice_row::~table_slice_row() noexcept = default;

table_slice_row::table_slice_row(const table_slice_row&) noexcept = default;

table_slice_row&
table_slice_row::operator=(const table_slice_row&) noexcept = default;

table_slice_row::table_slice_row(table_slice_row&&) noexcept = default;

table_slice_row&
table_slice_row::operator=(table_slice_row&&) noexcept = default;

table_slice_row::table_slice_row(table_slice slice, size_t row) noexcept

  : slice_{std::move(slice)}, row_{row} {
  // nop
}

data_view table_slice_row::operator[](size_t column) const {
  VAST_ASSERT(column < size());
  return slice_.at(row_, column);
}

size_t table_slice_row::size() const noexcept {
  return slice_.columns();
}

const table_slice& table_slice_row::slice() const noexcept {
  return slice_;
}

size_t table_slice_row::index() const noexcept {
  return row_;
}

} // namespace vast
