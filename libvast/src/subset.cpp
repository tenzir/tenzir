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

#include "vast/subset.hpp"
#include "vast/value.hpp"

namespace vast {

namespace {

auto cap(table_slice::size_type pos, table_slice::size_type num,
         table_slice::size_type last) {
  return num == table_slice::npos ? last : std::min(last, pos + num);
}

} // namespace <anonymous>

std::vector<value> subset(const table_slice& slice,
                          table_slice::size_type first_row,
                          table_slice::size_type num_rows,
                          table_slice::size_type first_col,
                          table_slice::size_type num_cols) {
  std::vector<value> result;
  if (first_col >= slice.columns() || first_row >= slice.rows())
    return result;
  auto col_begin = first_col;
  auto col_end = cap(first_col, num_cols, slice.columns());
  auto row_begin = first_row;
  auto row_end = cap(first_row, num_rows, slice.rows());
  auto value_layout = slice.layout(first_col, num_cols);
  for (auto row = row_begin; row < row_end; ++row) {
    vector xs;
    for (auto col = col_begin; col < col_end; ++col) {
      auto opt = slice.at(row, col);
      VAST_ASSERT(opt != caf::none);
      xs.emplace_back(materialize(*opt));
    }
    result.emplace_back(value::make(std::move(xs), value_layout));
  }
  return result;
}

} // namespace vast
