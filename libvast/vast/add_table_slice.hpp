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

#include <caf/atom.hpp>

#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"

namespace vast {

/// Register a table slice along with a builder type.
/// @tparam TableSlice The table slice type.
/// @tparam TableSliceBuilder The table slice builder that builds instances of
///         type `TableSlice`.
/// @param id The ID to associate with the table slice and builder factories.
/// @returns `true` if the factory registration succeeded.
template <class TableSlice, class TableSliceBuilder>
bool add_table_slice() {
  constexpr auto id = TableSlice::class_id;
  // Make sure the ID isn't registered in both factories.
  if (get_table_slice_factory(id) != nullptr
      || get_table_slice_builder_factory(id) != nullptr)
    return false;
  // Add the table slice and builder factories.
  add_table_slice_factory<TableSlice>();
  add_table_slice_builder_factory<TableSliceBuilder>(id);
  return true;
}

} // namespace vast
