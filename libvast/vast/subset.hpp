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

#include "vast/table_slice.hpp"

namespace vast {

/// Performs a selection (and optional projection) on a table slice.
/// @param slice The table slice to subset.
/// @param first_row The row of the first value.
/// @param num_rows The number of values to select starting from *first_fow*.
/// @param first_col The first column to begin selecion
/// @param num_cols The number of columns from *first_col*.
/// @returns A subset of *slice*, with rows in the range
///          *[first_row, first_row + num_rows)* and columns in the range
///          *[first_col, first_col + num_cols)*.
/// @see subset_events
std::vector<value> subset(
  const table_slice& slice,
  table_slice::size_type first_row = 0u,
  table_slice::size_type num_rows = table_slice::npos,
  table_slice::size_type first_col = 0u,
  table_slice::size_type num_cols = table_slice::npos);

} // namespace vast
