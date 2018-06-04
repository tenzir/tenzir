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

#include "vast/detail/flat_set.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/partition_index.hpp"
#include "vast/time.hpp"
#include "vast/uuid.hpp"

namespace vast {

class event;

namespace system {

struct active_partition_state {
  uuid id;
  caf::actor partition;
  size_t events = 0;
};

struct scheduled_partition_state {
  uuid id;
  detail::flat_set<uuid> lookups;
};

struct lookup_state {
  expression expr;
  caf::actor sink;
  std::vector<uuid> partitions;
};

struct index_state {
  partition_index part_index;
  active_partition_state active;
  std::unordered_map<uuid, caf::actor> loaded;
  std::unordered_map<caf::actor, uuid> evicted;
  std::deque<scheduled_partition_state> scheduled;
  std::unordered_map<uuid, lookup_state> lookups;
  size_t capacity;
  path dir;
  static inline const char* name = "index";
};

/// Indexes events in horizontal partitions.
/// @param dir The directory of the index.
/// @param max_events The maximum number of events per partition.
/// @param max_parts The maximum number of partitions to hold in memory.
/// @param taste_parts The number of partitions to schedule immediately for
///                    each query
/// @pre `max_events > 0 && max_parts > 0`
caf::behavior index(caf::stateful_actor<index_state>* self, const path& dir,
                    size_t max_events, size_t max_parts, size_t taste_parts);

} // namespace system
} // namespace vast

