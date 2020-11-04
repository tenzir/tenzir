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

#include "vast/detail/assert.hpp"
#include "vast/table_slice.hpp"

namespace vast {

table_slice_column::table_slice_column() noexcept = default;

table_slice_column::~table_slice_column() noexcept = default;

table_slice_column::table_slice_column(
  const table_slice_column&) noexcept = default;

table_slice_column&
table_slice_column::operator=(const table_slice_column&) noexcept = default;

table_slice_column::table_slice_column(table_slice_column&&) noexcept = default;

table_slice_column&
table_slice_column::operator=(table_slice_column&&) noexcept = default;

table_slice_column::table_slice_column(table_slice_ptr slice,
                                       size_t column) noexcept

  : slice_{std::move(slice)}, column_{column} {
  // nop
}

std::optional<table_slice_column>
table_slice_column::make(table_slice_ptr slice,
                         std::string_view column) noexcept {
  auto&& layout = slice->layout();
  for (size_t i = 0; i < layout.fields.size(); ++i)
    if (layout.fields[i].name == column)
      return table_slice_column{std::move(slice), i};
  return std::nullopt;
}

data_view table_slice_column::operator[](size_t row) const {
  VAST_ASSERT(row < size());
  return slice_->at(row, column_);
}

size_t table_slice_column::size() const noexcept {
  return slice_->rows();
}

const table_slice_ptr& table_slice_column::slice() const noexcept {
  return slice_;
}

size_t table_slice_column::index() const noexcept {
  return column_;
}

} // namespace vast
