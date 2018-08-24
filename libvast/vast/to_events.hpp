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

#pragma once

#include <vector>

#include "vast/fwd.hpp"
#include "vast/table_slice.hpp"

namespace vast {

/// Performs a selection of events on a table slice.
/// @param storage List for storing a selection of *slice* with rows in the
///                range *[first_row, first_row + num_rows)*.
/// @param slice The table slice to subset.
/// @param first_row The row of the first event.
/// @param num_rows The number of events to select starting from *first_fow*.
/// @see subset
void to_events(std::vector<event>& storage, const table_slice& slice,
               table_slice::size_type first_row = 0u,
               table_slice::size_type num_rows = table_slice::npos);

/// Performs a selection of events on a table slice.
/// @param slice The table slice to subset.
/// @param first_row The row of the first event.
/// @param num_rows The number of events to select starting from *first_fow*.
/// @returns A selection of *slice* with rows in the range
///          *[first_row, first_row + num_rows)*
/// @see subset
std::vector<event> to_events(
  const table_slice& slice,
  table_slice::size_type first_row = 0u,
  table_slice::size_type num_rows = table_slice::npos);

/// Performs a selection of events on a table slice.
/// @param storage List for storing a selection of *slice*.
/// @param slice The table slice to subset.
/// @param row_ids IDs of individual rows.
/// @see subset
void to_events(std::vector<event>& storage, const table_slice& slice,
               const ids& row_ids);

/// Performs a selection of events on a table slice.
/// @param slice The table slice to subset.
/// @param row_ids IDs of individual rows.
/// @returns A selection of *slice*.
/// @see subset
std::vector<event> to_events(const table_slice& slice, const ids& row_ids);

} // namespace vast
