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

#include "vast/table.hpp"

#include <caf/none.hpp>
#include <caf/optional.hpp>

#include "vast/const_table_slice_handle.hpp"
#include "vast/table_slice.hpp"

namespace vast {

table::table(record_type layout)
  : layout_(std::move(layout)),
    columns_(flat_size(layout_)) {
  // nop
}

bool table::add(const_table_slice_handle slice) {
  if (slice == nullptr || layout_ != slice->layout())
    return false;
  auto offset = slice->offset();
  slices_.emplace_back(offset, std::move(slice));
  return true;
}

caf::optional<data_view> table::at(size_type row, size_type col) const {
  auto pred = [&](const value_type& x) {
    return x.first >= row && row < x.first + x.second->rows();
  };
  auto i = std::find_if(slices_.begin(), slices_.end(), pred);
  if (i != slices_.end())
    return i->second->at(row - i->first, col);
  return caf::none;
}

} // namespace vast
