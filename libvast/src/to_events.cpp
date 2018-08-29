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

#include "vast/to_events.hpp"

#include "vast/bitmap_algorithms.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/subset.hpp"

namespace vast {

namespace {

event to_event(value& val, value& tstamp, const std::string& layout_name,
               id event_id) {
  using caf::get;
  auto t = val.type().name(layout_name);
  auto e = event::make(std::move(val.data()), std::move(t));
  e.id(event_id);
  e.timestamp(get<timestamp>(get<vector>(tstamp)[0]));
  return e;
}

} // namespace <anonymous>

void to_events(std::vector<event>& storage, const table_slice& slice,
               table_slice::size_type first_row,
               table_slice::size_type num_rows) {
  auto values = subset(slice, first_row, num_rows, 1);
  auto timestamps = subset(slice, first_row, num_rows, 0, 1);
  VAST_ASSERT(values.size() == timestamps.size());
  storage.reserve(storage.size() + values.size());
  auto event_id = slice.offset() + first_row;
  for (size_t i = 0; i < values.size(); ++i)
    storage.emplace_back(to_event(values[i], timestamps[i],
                                  slice.layout().name(), event_id++));
}

std::vector<event> to_events(const table_slice& slice,
                             table_slice::size_type first_row,
                             table_slice::size_type num_rows) {
  std::vector<event> result;
  to_events(result, slice, first_row, num_rows);
  return result;
}

void to_events(std::vector<event>& storage, const table_slice& slice,
               const ids& row_ids) {
  auto begin = slice.offset();
  auto end = begin + slice.rows();
  auto rng = select(row_ids);
  VAST_ASSERT(rng);
  for (rng.next_at(begin); rng && rng.get() < end; rng.next()) {
    auto id = rng.get();
    // FIXME: inefficient, because we wrap single values into vectors
    auto timestamps = subset(slice, id - begin, 1, 0, 1);
    auto values = subset(slice, id - begin, 1, 1);
    VAST_ASSERT(timestamps.size() == 1);
    VAST_ASSERT(values.size() == 1);
    auto& name = slice.layout().name();
    storage.emplace_back(to_event(values[0], timestamps[0], name, id));
  }
}

std::vector<event> to_events(const table_slice& slice, const ids& row_ids) {
  std::vector<event> result;
  to_events(result, slice, row_ids);
  return result;
}

} // namespace vast
