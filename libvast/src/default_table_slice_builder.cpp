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

#include "vast/default_table_slice_builder.hpp"

#include <utility>

#include <caf/make_counted.hpp>

namespace vast {

default_table_slice_builder::default_table_slice_builder(record_type layout)
  : super{std::move(layout)},
    row_(super::layout().fields.size()),
    col_{0} {
  VAST_ASSERT(!row_.empty());
}

table_slice_builder_ptr default_table_slice_builder::make(record_type layout) {
  return caf::make_counted<default_table_slice_builder>(std::move(layout));
}

bool default_table_slice_builder::append(data x) {
  lazy_init();
  // TODO: consider an unchecked version for improved performance.
  if (!type_check(layout().fields[col_].type, x))
    return false;
  row_[col_++] = std::move(x);
  if (col_ == layout().fields.size()) {
    slice_->xs_.push_back(std::move(row_));
    row_ = vector(slice_->columns());
    col_ = 0;
  }
  return true;
}

bool default_table_slice_builder::add(data_view x) {
  return append(materialize(x));
}

table_slice_ptr default_table_slice_builder::finish() {
  // If we have an incomplete row, we take it as-is and keep the remaining null
  // values. Better to have incomplete than no data.
  if (col_ != 0)
    slice_->xs_.push_back(std::move(row_));
  // Populate slice.
  slice_->header_.rows = slice_->xs_.size();
  return table_slice_ptr{slice_.release(), false};
}

size_t default_table_slice_builder::rows() const noexcept {
  return slice_ == nullptr ? 0u : slice_->xs_.size();
}

void default_table_slice_builder::reserve(size_t num_rows) {
  lazy_init();
  slice_->xs_.reserve(num_rows);
}

caf::atom_value
default_table_slice_builder::implementation_id() const noexcept {
  return default_table_slice::class_id;
}

void default_table_slice_builder::lazy_init() {
  if (slice_ == nullptr) {
    table_slice_header header;
    header.layout = layout();
    slice_.reset(new default_table_slice{std::move(header)});
    row_ = vector(slice_->columns());
    col_ = 0;
  }
}

} // namespace vast
