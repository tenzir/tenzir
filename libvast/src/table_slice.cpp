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

#include "vast/table_slice.hpp"

#include "caf/sum_type.hpp"

#include "vast/detail/overload.hpp"
#include "vast/value.hpp"

namespace vast {

table_slice::table_slice(record_type layout)
  : layout_(std::move(layout)),
    rows_(0),
    columns_(flat_size(layout_)) {
  // nop
}

table_slice::~table_slice() {
  // no
}

caf::optional<value> table_slice::row_to_value(size_type row,
                                               size_type first_column,
                                               size_type num_columns) const {
  auto result = rows_to_values(row, 1, first_column, num_columns);
  if (result.empty())
    return caf::none;
  VAST_ASSERT(result.size() == 1u);
  return std::move(result.front());
}

std::vector<value> table_slice::rows_to_values(size_type first_row,
                                               size_type num_rows,
                                               size_type first_column,
                                               size_type num_columns) const {
  auto cap = [](size_type pos, size_type num, size_type last) {
    return num == npos ? last : std::min(last, pos + num);
  };
  std::vector<value> result;
  if (first_column >= columns_ || first_row >= rows_)
    return result;
  auto col_begin = first_column;
  auto col_end = cap(first_column, num_columns, columns_);
  auto row_begin = first_row;
  auto row_end = cap(first_row, num_rows, rows_);
  std::vector<record_field> sub_records{layout_.fields.begin() + col_begin,
                                        layout_.fields.begin() + col_end};
  record_type value_layout{std::move(sub_records)};
  for (size_type row = row_begin; row < row_end; ++row) {
    vector xs;
    for (size_type col = col_begin; col < col_end; ++col) {
      auto opt = at(row, col);
      VAST_ASSERT(opt != caf::none);
      xs.emplace_back(materialize(*opt));
    }
    result.emplace_back(value::make(std::move(xs), value_layout));
  }
  return result;
}

} // namespace vast
