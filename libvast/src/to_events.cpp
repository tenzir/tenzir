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

#include <algorithm>

#include <caf/optional.hpp>

#include "vast/bitmap_algorithms.hpp"
#include "vast/event.hpp"
#include "vast/ids.hpp"
#include "vast/subset.hpp"

namespace vast {

namespace {

optional<size_t> find_time_column(record_type layout) {
  auto result = caf::optional<size_t>{caf::none};
  for (size_t i = 0; i < layout.fields.size(); ++i)
    if (has_attribute(layout.fields[i].type, "time"))
      return i;
  return result;
}

event to_event(const table_slice& slice, id eid, type event_layout,
               caf::optional<size_t> timestamp_column) {
  VAST_ASSERT(slice.columns() > 0);
  VAST_ASSERT(!timestamp_column || *timestamp_column < slice.columns());
  vector xs(slice.columns());  // TODO(ch3290): make this a record
  for (table_slice::size_type i = 0; i < slice.columns(); ++i)
    xs[i] = materialize(slice.at(eid - slice.offset(), i));
  auto e = event::make(std::move(xs), std::move(event_layout));
  // Assign event meta data.
  e.id(eid);
  // Assign event timestamp.
  if (timestamp_column) {
    auto& xs = caf::get<vector>(e.data());
    auto ts = caf::get<timestamp>(xs[*timestamp_column]);
    e.timestamp(ts);
  }
  return e;
}

} // namespace <anonymous>

void to_events(std::vector<event>& storage, const table_slice& slice,
               table_slice::size_type first_row,
               table_slice::size_type num_rows) {
  if (num_rows == table_slice::npos)
    num_rows = slice.rows();
  // Figure out whether there's a column that could be the event timestamp.
  auto timestamp_column = find_time_column(slice.layout());
  for (auto i = first_row; i < first_row + num_rows; ++i)
    storage.emplace_back(to_event(slice, slice.offset() + i, slice.layout(),
                                  timestamp_column));
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
  if (rng.get() < begin)
    rng.next_from(begin);
  // Figure out whether there's a column that could be the event timestamp.
  auto timestamp_column = find_time_column(slice.layout());
  for ( ; rng && rng.get() < end; rng.next())
    storage.emplace_back(to_event(slice, rng.get(), slice.layout(),
                                  timestamp_column));
}

std::vector<event> to_events(const table_slice& slice, const ids& row_ids) {
  std::vector<event> result;
  to_events(result, slice, row_ids);
  return result;
}

} // namespace vast
