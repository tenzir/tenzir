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
#include "vast/status.hpp"
#include "vast/store.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/instrumentation.hpp"

#include <caf/fwd.hpp>
#include <caf/replies_to.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <chrono>
#include <memory>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace vast::system {

// clang-format off
using receiver_type = caf::typed_actor<
  caf::reacts_to<table_slice_ptr>,
  caf::reacts_to<atom::done, caf::error>
>;
// clang-format on

// clang-format off
/// @relates archive
using archive_type = caf::typed_actor<
  caf::reacts_to<caf::stream<table_slice_ptr>>,
  caf::reacts_to<atom::exporter, caf::actor>,
  caf::reacts_to<accountant_type>,
  caf::reacts_to<ids>,
  caf::reacts_to<ids, receiver_type>,
  caf::reacts_to<ids, receiver_type, uint64_t>,
  caf::replies_to<atom::status, status_verbosity>::with<caf::dictionary<caf::config_value>>,
  caf::reacts_to<atom::telemetry>,
  caf::reacts_to<atom::erase, ids>
>;
// clang-format on

/// @relates archive
struct archive_state {
  void send_report();
  void next_session();
  archive_type::stateful_pointer<archive_state> self;
  std::unique_ptr<vast::store> store;
  std::unique_ptr<vast::store::lookup> session;
  uint64_t session_id = 0;
  std::queue<receiver_type> requesters;
  std::unordered_map<caf::actor_addr, std::queue<ids>> unhandled_ids;
  std::unordered_set<caf::actor_addr> active_exporters;
  vast::system::measurement measurement;
  accountant_type accountant;
  static inline const char* name = "archive";
};

/// Stores event batches and answers queries for ID sets.
/// @param self The actor handle.
/// @param dir The root directory of the archive.
/// @param capacity The number of segments to cache in memory.
/// @param max_segment_size The maximum segment size in bytes.
/// @pre `max_segment_size > 0`
archive_type::behavior_type
archive(archive_type::stateful_pointer<archive_state> self, path dir,
        size_t capacity, size_t max_segment_size);

} // namespace vast::system
