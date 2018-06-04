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
  /// A closed interval.
  struct interval {
    timestamp from = timestamp::max();
    timestamp to = timestamp::min();
  };

  /// Per-partition summary statistics.
  struct partition_synopsis {
    interval range;
  };

  /// Returns the synopsis for a partition if present, returns `none` otherwise.
  caf::optional<partition_synopsis> operator[](const uuid& partition) const;

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

bool operator==(const partition_index::interval&,
                const partition_index::interval&);

inline bool operator!=(const partition_index::interval& x,
                       const partition_index::interval& y) {
  return !(x == y);
}

} // namespace vast::system
