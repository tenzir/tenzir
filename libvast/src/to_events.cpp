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

event to_event(const table_slice& slice, id eid, type event_layout) {
  vector xs;  // TODO(ch3290): make this a record
  VAST_ASSERT(slice.columns() > 0);
  xs.resize(slice.columns() - 1); // one less to exclude timestamp
  for (table_slice::size_type i = 0; i < slice.columns() - 1; ++i)
    xs[i] = materialize(slice.at(eid - slice.offset(), i + 1));
  auto e = event::make(std::move(xs), std::move(event_layout));
  // Get the timestamp from the first column.
  auto ts = slice.at(eid - slice.offset(), 0);
  // Assign event meta data.
  e.id(eid);
  e.timestamp(caf::get<timestamp>(ts));
  return e;
}

} // namespace <anonymous>

void to_events(std::vector<event>& storage, const table_slice& slice,
               table_slice::size_type first_row,
               table_slice::size_type num_rows) {
  auto event_layout = slice.layout(1).name(slice.layout().name());
  if (num_rows == table_slice::npos)
    num_rows = slice.rows();
  for (auto i = first_row; i < first_row + num_rows; ++i)
    storage.emplace_back(to_event(slice, slice.offset() + i, event_layout));
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
  auto event_layout = slice.layout(1).name(slice.layout().name());
  if (rng.get() < begin)
    rng.next_from(begin);
  for ( ; rng && rng.get() < end; rng.next())
    storage.emplace_back(to_event(slice, rng.get(), event_layout));
}

std::vector<event> to_events(const table_slice& slice, const ids& row_ids) {
  std::vector<event> result;
  to_events(result, slice, row_ids);
  return result;
}

} // namespace vast
