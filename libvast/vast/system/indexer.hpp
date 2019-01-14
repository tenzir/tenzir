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

#include <unordered_map>

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/column_index.hpp"
#include "vast/filesystem.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

namespace vast::system {

struct indexer_state {
  // -- constructors, destructors, and assignment operators --------------------

  indexer_state();

  ~indexer_state();

  caf::error init(caf::event_based_actor* self, path filename, type column_type,
                  size_t column, caf::actor index, uuid partition_id);

  // -- member variables -------------------------------------------------------

  union { column_index col; };

  caf::actor index;

  uuid partition_id;

  static inline const char* name = "indexer";
};

/// Indexes a single column of table slices.
/// @param self The actor handle.
/// @param dir The directory where to store the indexes in.
/// @param column_type The type of the indexed column.
/// @param column The indexed column.
/// @param index A handle to the index actor.
/// @param partition_id The partition ID that this INDEXER belongs to.
/// @returns the initial behavior of the INDEXER.
caf::behavior indexer(caf::stateful_actor<indexer_state>* self, path dir,
                      type column_type, size_t column, caf::actor index,
                      uuid partition_id);

} // namespace vast::system
