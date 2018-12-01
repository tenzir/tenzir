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

#include "vast/column_major_matrix_table_slice_builder.hpp"

#include <memory>

#include <caf/make_counted.hpp>

#include "vast/matrix_table_slice.hpp"

namespace vast {

caf::atom_value
column_major_matrix_table_slice_builder::get_implementation_id() noexcept {
  return column_major_matrix_table_slice::class_id;
}

column_major_matrix_table_slice_builder::
  column_major_matrix_table_slice_builder(record_type layout)
  : super(std::move(layout)),
    col_(0),
    rows_(0) {
  columns_.resize(columns());
}

column_major_matrix_table_slice_builder::
  ~column_major_matrix_table_slice_builder() {
  // nop
}

table_slice_builder_ptr
column_major_matrix_table_slice_builder::make(record_type layout) {
  using impl = column_major_matrix_table_slice_builder;
  return caf::make_counted<impl>(std::move(layout));
}

table_slice_ptr column_major_matrix_table_slice_builder::make_slice(
  record_type layout, table_slice::size_type rows) {
  using impl = column_major_matrix_table_slice;
  return table_slice_ptr{impl::make(std::move(layout), rows)};
}

bool column_major_matrix_table_slice_builder::append(data x) {
  // Check whether input is valid.
  if (!type_check(layout().fields[col_].type, x))
    return false;
  columns_[col_].emplace_back(std::move(x));
  if (col_ + 1 == columns_.size()) {
    ++rows_;
    col_ = 0;
  } else {
    ++col_;
  }
  return true;
}

bool column_major_matrix_table_slice_builder::add(data_view x) {
  return append(materialize(x));
}

table_slice_ptr column_major_matrix_table_slice_builder::finish() {
  // Sanity check.
  if (col_ != 0 || rows_ == 0)
    return {};
  // Get uninitialized memory that keeps the slice object plus the full matrix.
  using impl = column_major_matrix_table_slice;
  auto result = impl::make_uninitialized(layout(), rows_);
  // Construct the data block.
  auto data_ptr = result->elements();
  for (auto& col_vec : columns_) {
    VAST_ASSERT(col_vec.size() == rows_);
    std::uninitialized_move(col_vec.begin(), col_vec.end(), data_ptr);
    data_ptr += rows_;
    col_vec.clear();
  }
  rows_ = 0;
  return table_slice_ptr{result, false};
}

size_t column_major_matrix_table_slice_builder::rows() const noexcept {
  return rows_;
}

void column_major_matrix_table_slice_builder::reserve(size_t num_rows) {
  for (auto& col_vec : columns_)
    col_vec.reserve(num_rows);
}

caf::atom_value
column_major_matrix_table_slice_builder::implementation_id() const noexcept {
  return get_implementation_id();
}

} // namespace vast
