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

#include "vast/fwd.hpp"

#include "vast/ids.hpp"
#include "vast/store.hpp"
#include "vast/system/accountant_actor.hpp"
#include "vast/system/archive_actor.hpp"
#include "vast/system/instrumentation.hpp"

#include <memory>
#include <queue>
#include <unordered_map>
#include <unordered_set>

namespace vast::system {

/// @relates archive
struct archive_state {
  void send_report();
  void next_session();
  archive_actor::pointer self;
  std::unique_ptr<store> store;
  std::unique_ptr<store::lookup> session;
  uint64_t session_id = 0;
  std::queue<archive_client_actor> requesters;
  std::unordered_map<caf::actor_addr, std::queue<ids>> unhandled_ids;
  std::unordered_set<caf::actor_addr> active_exporters;
  vast::system::measurement measurement;
  accountant_actor accountant;
  static inline const char* name = "archive";
};

/// Stores event batches and answers queries for ID sets.
/// @param self The actor handle.
/// @param dir The root directory of the archive.
/// @param capacity The number of segments to cache in memory.
/// @param max_segment_size The maximum segment size in bytes.
/// @pre `max_segment_size > 0`
archive_actor::behavior_type
archive(archive_actor::stateful_pointer<archive_state> self, path dir,
        size_t capacity, size_t max_segment_size);

} // namespace vast::system
