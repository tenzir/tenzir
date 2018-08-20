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

#include "vast/event.hpp"
#include "vast/subset.hpp"
#include "vast/to_events.hpp"

namespace vast {

std::vector<event> to_events(const table_slice& slice,
                             table_slice::size_type first_row,
                             table_slice::size_type num_rows) {
  using caf::get;
  auto values = subset(slice, first_row, num_rows, 1);
  auto timestamps = subset(slice, first_row, num_rows, 0, 1);
  VAST_ASSERT(values.size() == timestamps.size());
  std::vector<event> result;
  result.reserve(values.size());
  auto event_id = slice.offset() + first_row;
  for (size_t i = 0; i < values.size(); ++i) {
    auto t = values[i].type().name(slice.layout().name());
    result.emplace_back(event::make(std::move(values[i].data()), std::move(t)));
    result.back().id(event_id++);
    result.back().timestamp(get<timestamp>(get<vector>(timestamps[i])[0]));
  }
  return result;
}

} // namespace vast
