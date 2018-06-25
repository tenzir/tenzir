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
#include <caf/stateful_actor.hpp>

#include "vast/filesystem.hpp"
#include "vast/system/table_index.hpp"
#include "vast/type.hpp"

namespace vast::system {

struct indexer_state {
  indexer_state();
  ~indexer_state();
  void init(table_index&& from);
  union { table_index tbl; };
  bool initialized;
  static inline const char* name = "indexer";
};

/// Indexes an event.
/// @param self The actor handle.
/// @param dir The directory where to store the indexes in.
/// @param type event_type The type of the event to index.
caf::behavior indexer(caf::stateful_actor<indexer_state>* self, path dir,
                      type event_type);

} // namespace vast::system

