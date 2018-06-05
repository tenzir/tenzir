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
#include <vector>

#include <caf/actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/detail/flat_lru_cache.hpp"
#include "vast/detail/flat_set.hpp"
#include "vast/expression.hpp"
#include "vast/fwd.hpp"
#include "vast/system/indexer_stage_driver.hpp"
#include "vast/system/partition.hpp"
#include "vast/system/partition_index.hpp"
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

  /// Pointer to the stage that multiplexing traffic between our sources and
  /// the INDEXER actors of the current partition.
  using stage_ptr = indexer_stage_driver::stage_ptr_type;

  /// Looks up partitions in the LRU cache by UUID.
  class partition_lookup {
  public:
    inline auto operator()(const uuid& id) const {
      return [&](const partition_ptr& ptr) {
        return ptr->id() == id;
      };
    }
  };

  /// Loads partitions from disk by UUID.
  class partition_factory {
  public:
    inline partition_factory(index_state* st) : st_(st) {
      // nop
    }

    partition_ptr operator()(const uuid& id) const;

  private:
    index_state* st_;
  };

  using partition_cache_type = detail::flat_lru_cache<partition_ptr,
                                                      partition_lookup,
                                                      partition_factory>;

  // -- constructors, destructors, and assignment operators --------------------

  index_state();

  /// Initializes the state.
  void init(caf::event_based_actor* self, const path& dir, size_t max_events,
            size_t max_parts, size_t taste_parts);

  // -- member variables -------------------------------------------------------

  /// Allows to select partitions with timestamps.
  partition_index part_index;

  /// Our current partition.
  partition_ptr active;

  /// Recently accessed partitions.
  partition_cache_type partition_cache;

  /// Base directory for all partitions of the index.
  path dir;

  /// Stream manager for ingesting events.
  stage_ptr stage;

  /// Pointer to the partent actor.
  caf::event_based_actor* self;

  /// The maximum number of events per partition.
  size_t partition_size;

  /// The number of partitions to schedule immediately for each query
  size_t taste_partitions;

  /// Name of the INDEX actor.
  static inline const char* name = "index";

  /// Stores the next available worker for queries.
  caf::actor next_worker;

  /// Allows the index to multiplex between waiting for ready workers and
  /// queries.
  caf::behavior has_worker;
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

