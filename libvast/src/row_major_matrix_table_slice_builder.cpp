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

#include "vast/row_major_matrix_table_slice_builder.hpp"

#include <memory>

#include <caf/make_counted.hpp>

#include "vast/matrix_table_slice.hpp"

namespace vast {

row_major_matrix_table_slice_builder ::row_major_matrix_table_slice_builder(
  record_type layout)
  : super(std::move(layout)),
    col_(0) {
  // nop
}

row_major_matrix_table_slice_builder::~row_major_matrix_table_slice_builder() {
  // nop
}

table_slice_builder_ptr
row_major_matrix_table_slice_builder::make(record_type layout) {
  using impl = row_major_matrix_table_slice_builder;
  return caf::make_counted<impl>(std::move(layout));
}

table_slice_ptr
row_major_matrix_table_slice_builder::make_slice(record_type layout,
                                                 table_slice::size_type rows) {
  using impl = row_major_matrix_table_slice;
  return table_slice_ptr{impl::make(std::move(layout), rows)};
}

bool row_major_matrix_table_slice_builder::append(data x) {
  // Check whether input is valid.
  if (!type_check(layout().fields[col_].type, x))
    return false;
  col_ = (col_ + 1) % columns();
  elements_.emplace_back(std::move(x));
  return true;
}

bool row_major_matrix_table_slice_builder::add(data_view x) {
  return append(materialize(x));
}

table_slice_ptr row_major_matrix_table_slice_builder::finish() {
  // Sanity check.
  if (col_ != 0)
    return {};
  // Get uninitialized memory that keeps the slice object plus the full matrix.
  using impl = row_major_matrix_table_slice;
  auto result = impl::make_uninitialized(layout(), rows());
  // Construct the data block.
  std::uninitialized_move(elements_.begin(), elements_.end(),
                          result->elements());
  // Clean up the builder state and return.
  elements_.clear();
  return table_slice_ptr{result, false};
}

size_t row_major_matrix_table_slice_builder::rows() const noexcept {
  return elements_.size() / columns();
}

void row_major_matrix_table_slice_builder::reserve(size_t num_rows) {
  elements_.reserve(num_rows * columns());
}

caf::atom_value
row_major_matrix_table_slice_builder::implementation_id() const noexcept {
  return get_implementation_id();
}

caf::atom_value
row_major_matrix_table_slice_builder::get_implementation_id() noexcept {
  return row_major_matrix_table_slice::class_id;
}

} // namespace vast
