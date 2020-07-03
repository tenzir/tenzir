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

#include "vast/caf_table_slice_builder.hpp"

#include <caf/make_counted.hpp>

#include <utility>

namespace vast {

caf::atom_value caf_table_slice_builder::get_implementation_id() noexcept {
  return caf::atom("caf");
}

caf_table_slice_builder::caf_table_slice_builder(record_type layout)
  : super{std::move(layout)}, row_(super::layout().fields.size()), col_{0} {
  VAST_ASSERT(!row_.empty());
}

table_slice_builder_ptr caf_table_slice_builder::make(record_type layout) {
  return caf::make_counted<caf_table_slice_builder>(std::move(layout));
}

bool caf_table_slice_builder::append(data x) {
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

bool caf_table_slice_builder::add_impl(data_view x) {
  return append(materialize(x));
}

table_slice_ptr caf_table_slice_builder::finish() {
  // If we have an incomplete row, we take it as-is and keep the remaining null
  // values. Better to have incomplete than no data.
  if (col_ != 0)
    slice_->xs_.push_back(std::move(row_));
  // Populate slice.
  slice_->header_.rows = slice_->xs_.size();
  return table_slice_ptr{slice_.release(), false};
}

size_t caf_table_slice_builder::rows() const noexcept {
  return slice_ == nullptr ? 0u : slice_->xs_.size();
}

void caf_table_slice_builder::reserve(size_t num_rows) {
  lazy_init();
  slice_->xs_.reserve(num_rows);
}

caf::atom_value caf_table_slice_builder::implementation_id() const noexcept {
  return caf_table_slice::class_id;
}

void caf_table_slice_builder::lazy_init() {
  if (slice_ == nullptr) {
    table_slice_header header;
    header.layout = layout();
    slice_.reset(new caf_table_slice{std::move(header)});
    row_ = vector(slice_->columns());
    col_ = 0;
  }
}

} // namespace vast
