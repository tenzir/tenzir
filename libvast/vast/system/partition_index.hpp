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

#include <caf/optional.hpp>

#include "vast/fwd.hpp"
#include "vast/time.hpp"
#include "vast/uuid.hpp"

namespace vast::system {

/// Maps events to horizontal partitions of the ::index.
class partition_index {
public:
  // -- member types -----------------------------------------------------------

  /// A closed interval.
  struct interval {
    timestamp from = timestamp::max();
    timestamp to = timestamp::min();
  };

  /// Per-partition summary statistics.
  struct partition_synopsis {
    interval range;
  };

  using map_type = std::unordered_map<uuid, partition_synopsis>;

  using const_iterator = map_type::const_iterator;

  // -- properties -------------------------------------------------------------

  /// Returns the synopsis for a partition if present, returns `none` otherwise.
  caf::optional<partition_synopsis> operator[](const uuid& partition) const;

  /// Adds a set of events to the index for a given partition.
  template <class EventIterator>
  void add(const uuid& partition, EventIterator first, EventIterator last) {
    auto& rng = partitions_[partition].range;
    for_each(first, last, [&](const event& x) { add_one(rng, x); });
  }

  template <class Container>
  void add(const uuid& partition, const Container& xs) {
    add(partition, xs.begin(), xs.end());
  }

  /// Retrieves the list of partition IDs for a given expression.
  std::vector<uuid> lookup(const expression& expr) const;

  inline size_t size() const {
    return partitions_.size();
  }

  inline const_iterator begin() const {
    return partitions_.begin();
  }

  inline const_iterator end() const {
    return partitions_.end();
  }

  // -- inspection -------------------------------------------------------------

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
  // -- utility ----------------------------------------------------------------

  /// Adds `x` to the time interval `rng`.
  static void add_one(interval& rng, const event& x);

  // -- member variables -------------------------------------------------------

  map_type partitions_;
};

bool operator==(const partition_index::interval&,
                const partition_index::interval&);

inline bool operator!=(const partition_index::interval& x,
                       const partition_index::interval& y) {
  return !(x == y);
}

} // namespace vast::system
