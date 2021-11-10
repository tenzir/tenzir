//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/table_slice_column.hpp"

#include "vast/detail/assert.hpp"
#include "vast/table_slice.hpp"

namespace vast {

table_slice_column::table_slice_column() noexcept = default;

table_slice_column::~table_slice_column() noexcept = default;

table_slice_column::table_slice_column(const table_slice_column&) = default;

table_slice_column& table_slice_column::operator=(const table_slice_column&)
  = default;

table_slice_column::table_slice_column(table_slice_column&&) noexcept = default;

table_slice_column&
table_slice_column::operator=(table_slice_column&&) noexcept = default;

table_slice_column::table_slice_column(table_slice slice,
                                       size_t column) noexcept
  : slice_{std::move(slice)}, column_{column} {
  // nop
}

std::optional<table_slice_column>
table_slice_column::make(table_slice slice, std::string_view column) noexcept {
  const auto layout = caf::get<record_type>(slice.layout());
  // TODO: Doesn't it make way more sense to resolve the column name exactly
  // here instead of taking the first column that suffix-matches the name?
  const auto offsets = layout.resolve_key_suffix(column);
  if (offsets.empty())
    return std::nullopt;
  return table_slice_column{
    std::move(slice),
    layout.flat_index(offsets.front()),
  };
}

data_view table_slice_column::operator[](size_t row) const {
  VAST_ASSERT(row < size());
  return slice_.at(row, column_);
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

} // namespace vast
