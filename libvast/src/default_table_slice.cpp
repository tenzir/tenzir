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

#include <caf/make_counted.hpp>

#include "vast/default_table_slice.hpp"

namespace vast {

default_table_slice::default_table_slice(record_type layout)
  : table_slice{std::move(layout)} {
  // nop
}

data default_table_slice::at(size_type row, size_type col) const {
  VAST_ASSERT(row < rows_);
  VAST_ASSERT(row < xs_.size());
  VAST_ASSERT(col < columns_);
  if (auto x = get_if<vector>(xs_[row])) {
    VAST_ASSERT(col < x->size());
    return (*x)[col];
  }
  return {};
}

default_table_slice::builder_ptr default_table_slice::builder::make(record_type layout) {
  return caf::make_counted<builder>(std::move(layout));
}

default_table_slice::builder::builder(record_type layout)
  : layout_{flatten(layout)},
    row_(layout_.fields.size()),
    col_{0} {
  VAST_ASSERT(!row_.empty());
}

bool default_table_slice::builder::add(const data& x) {
  if (!slice_) {
    slice_ = caf::make_counted<default_table_slice>(layout_);
    row_ = vector(layout_.fields.size());
    col_ = 0;
  }
  // TODO: consider an unchecked version for improved performance.
  if (!type_check(layout_.fields[col_].type, x))
      return false;
  row_[col_++] = x;
  if (col_ == layout_.fields.size()) {
    slice_->xs_.push_back(std::move(row_));
    row_ = vector(layout_.fields.size());
    col_ = 0;
  }
  return true;
}

table_slice_ptr default_table_slice::builder::finish() {
  // If we have an incomplete row, we take it as-is and keep the remaining null
  // values. Better to have incomplete than no data.
  if (col_ != 0)
    slice_->xs_.push_back(std::move(row_));
  // Populate slice.
  // TODO: this feels messy, but allows for non-virtual parent accessors.
  slice_->rows_ = slice_->xs_.size();
  slice_->columns_ = layout_.fields.size();
  auto result = slice_;
  slice_ = {};
  return result;
};

} // namespace vast
