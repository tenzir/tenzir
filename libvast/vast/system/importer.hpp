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

#include <chrono>
#include <vector>

#include <caf/stateful_actor.hpp>

#include "vast/aliases.hpp"
#include "vast/data.hpp"
#include "vast/event.hpp"
#include "vast/filesystem.hpp"

#include "vast/system/archive.hpp"
#include "vast/system/meta_store.hpp"

namespace vast::system {

/// Receives chunks from SOURCEs, imbues them with an ID, and relays them to
/// ARCHIVE, INDEX and continuous queries.
struct importer_state {
  /// A simple generator for IDs.
  struct id_generator {
    /// The next available ID.
    id i;

    /// The first unavailable ID.
    id last;

    inline id_generator(id from, id to) : i(from), last(to) {
      // nop
    }

    /// Returns whether this generator is exhausted.
    inline bool at_end() const noexcept {
      return i == last;
    }

    /// Returns the next ID and advances the position in the range.
    inline id next() noexcept {
      return i++;
    }

    /// Returns how many more times `next` returns a valid ID.
    inline int32_t remaining() const noexcept {
      return static_cast<int32_t>(last - i);
    }
  };

  /// Handle to the meta store for obtaining more IDs.
  meta_store_type meta_store;

  /// Handle to the ARCHIVE for persisting incoming events.
  caf::actor archive;

  /// Handle to the INDEX for forwarding incoming events.
  caf::actor index;

  /// Stores currently available IDs.
  std::vector<id_generator> id_generators;

  /// Returns the number of currently available IDs.
  int32_t available_ids() const noexcept;

  /// Returns the next available IDs.
  /// @pre `available_ids() > 0`
  id next_id();

  /// Stores how many events inbound paths can still send us.
  int32_t in_flight_events = 0;

  /// Number of IDs we acquire per replenish.
  size_t id_chunk_size = 1024;

  /// Stores when we received new IDs for the last time.
  std::chrono::steady_clock::time_point last_replenish;

  /// Stores continous queries that receive new events in the same way ARCHIVE
  /// and INDEX do.
  std::vector<caf::actor> continuous_queries;

  /// State directory.
  path dir;

  /// Stores whether we've contacted the meta store to obtain more IDs.
  bool awaiting_ids = false;

  /// Cache for buffering events for ARCHIVE, INDEX and continuous queries.
  std::vector<event> remainder;

  /// Name of this actor in log events.
  static inline const char* name = "importer";
};

/// Spawns an IMPORTER.
/// @param self The actor handle.
/// @param dir The directory for persistent state.
/// @param batch_size The initial number of IDs to request when replenishing.
caf::behavior importer(caf::stateful_actor<importer_state>* self, path dir);

} // namespace vast::system

