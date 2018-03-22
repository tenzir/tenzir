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

#ifndef VAST_INDEX_HPP
#define VAST_INDEX_HPP

#include <unordered_map>

#include <caf/actor.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/uuid.hpp"
#include "vast/time.hpp"

#include "vast/detail/flat_set.hpp"

namespace vast {

class event;

namespace system {

/// Maps events to horizontal partitions of the ::index.
class partition_index {
public:
  /// A closed interval.
  struct interval {
    timestamp from = timestamp::max();
    timestamp to = timestamp::min();
  };

  /// Per-partition summary statistics.
  struct partition_synopsis {
    interval range;
  };

  /// Adds a set of events to the index for a given partition.
  void add(const std::vector<event> xs, const uuid& partition);

  /// Retrieves the list of partition IDs for a given expression.
  std::vector<uuid> lookup(const expression& expr) const;

  template <class Inspector>
  friend auto inspect(Inspector& f, interval& i) {
    return f(i.from, i.to);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, partition_synopsis& ps) {
    return f(ps.range);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, partition_index& pi) {
    return f(pi.partitions_);
  }

private:
  std::unordered_map<uuid, partition_synopsis> partitions_;
};

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

#endif
