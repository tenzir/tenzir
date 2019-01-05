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
  std::vector<data> xs(columns_.size() * rows_);
  auto x = xs.data();
  for (auto& column : columns_) {
    VAST_ASSERT(column.size() == rows_);
    for (size_t i = 0; i < rows_; ++i)
      *x++ = std::move(column[i]);
    column.clear();
  }
  rows_ = 0;
  return column_major_matrix_table_slice::make(layout(), std::move(xs));
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
