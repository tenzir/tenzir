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

#ifndef VAST_SYSTEM_IMPORTER_HPP
#define VAST_SYSTEM_IMPORTER_HPP

#include <chrono>
#include <vector>

#include <caf/stateful_actor.hpp>

#include "vast/aliases.hpp"
#include "vast/data.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"

#include "vast/system/archive.hpp"
#include "vast/system/meta_store.hpp"

namespace vast {
namespace system {

/// Receives chunks from SOURCEs, imbues them with an ID, and relays them to
/// ARCHIVE and INDEX.
struct importer_state {
  meta_store_type meta_store;
  caf::actor archive;
  caf::actor index;
  event_id next = 0;
  event_id available = 0;
  size_t batch_size;
  std::chrono::steady_clock::time_point last_replenish;
  std::vector<event> remainder;
  path dir;
  const char* name = "importer";
};

/// Spawns an IMPORTER.
/// @param self The actor handle.
/// @param dir The directory for persistent state.
/// @param batch_size The initial number of IDs to request when replenishing.
caf::behavior importer(caf::stateful_actor<importer_state>* self,
                       path dir, size_t batch_size);

} // namespace system
} // namespace vast

#endif
