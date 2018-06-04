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
#include "vast/fwd.hpp"
#include "vast/system/indexer_stage_driver.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/partition_index.hpp"
#include "vast/time.hpp"
#include "vast/uuid.hpp"

namespace vast::system {

struct scheduled_partition_state {
  uuid id;
  detail::flat_set<uuid> lookups;
};

struct lookup_state {
  expression expr;
  caf::actor sink;
  std::vector<uuid> partitions;
};

/// State of an INDEX actor.
struct index_state {
  // -- member types -----------------------------------------------------------

  //using stage_ptr = caf::stream_stage<event, indexer_downstream_manager>;
  using stage_ptr = indexer_stage_driver::stage_ptr_type;

  // -- constructors, destructors, and assignment operators --------------------

  void init(caf::event_based_actor* self, const path& dir, size_t max_events,
            size_t max_parts, size_t taste_parts);

  // -- member variables -------------------------------------------------------

  /// Allows to select partitions with timestamps.
  partition_index part_index;

  partition_ptr active;

  std::unordered_map<uuid, partition_ptr> loaded;

  std::unordered_map<partition_ptr, uuid> evicted;

  std::deque<scheduled_partition_state> scheduled;

  std::unordered_map<uuid, lookup_state> lookups;

  size_t capacity;

  path dir;

  /// Stream manager for ingesting events.
  stage_ptr stage;

  /// Pointer to the partent actor.
  caf::event_based_actor* self;

  /// The maximum number of events per partition.
  size_t partition_size;

  /// The maximum number of partitions to hold in memory.
  size_t in_mem_partitions;

  /// The number of partitions to schedule immediately for each query
  size_t taste_partitions;

  /// Name of the INDEX actor.
  static inline const char* name = "index";
};

/// Indexes events in horizontal partitions.
/// @param dir The directory of the index.
/// @param partition_size The maximum number of events per partition.
/// @param in_mem_partitions The maximum number of partitions to hold in memory.
/// @param taste_partitions The number of partitions to schedule immediately
///                         for each query
/// @pre `partition_size > 0 && in_mem_partitions > 0`
caf::behavior index(caf::stateful_actor<index_state>* self, const path& dir,
                    size_t partition_size, size_t in_mem_partitions,
                    size_t taste_partitions);

} // namespace vast::system

