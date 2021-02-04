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

table_slice_column::table_slice_column(table_slice slice, size_t column,
                                       qualified_record_field field) noexcept

  : slice_{std::move(slice)}, column_{column}, field_{std::move(field)} {
}

std::optional<table_slice_column>
table_slice_column::make(table_slice slice, std::string_view column) noexcept {
  auto&& layout = slice.layout();
  const auto offsets = layout.find_suffix(column);
  if (offsets.empty())
    return std::nullopt;
  auto offset = offsets.front();
  auto i = layout.flat_index_at(offset);
  VAST_ASSERT(i);
  return table_slice_column{
    std::move(slice), *i,
    qualified_record_field(layout.name(), *layout.flat_field_at(offset))};
}

data_view table_slice_column::operator[](size_t row) const {
  VAST_ASSERT(row < size());
  return slice_.at(row, column_, field_.type);
}

size_t table_slice_column::size() const noexcept {
  return slice_.rows();
}

const table_slice& table_slice_column::slice() const noexcept {
  return slice_;
}

size_t table_slice_column::index() const noexcept {
  return column_;
}

const qualified_record_field& table_slice_column::field() const noexcept {
  return field_;
}

} // namespace vast
