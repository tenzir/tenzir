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
#include "vast/type.hpp"

namespace vast::system {

struct event_indexer_state {
  path dir;
  type event_type;
  std::unordered_map<path, caf::actor> indexers;
  static inline const char* name = "event-indexer";
};

/// Indexes an event.
/// @param self The actor handle.
/// @param dir The directory where to store the indexes in.
/// @param type event_type The type of the event to index.
caf::behavior event_indexer(caf::stateful_actor<event_indexer_state>* self,
                            path dir, type event_type);

} // namespace vast::system

