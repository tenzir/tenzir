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

#include <tuple>
#include <unordered_map>

#include <caf/optional.hpp>
#include <caf/meta/type_name.hpp>

#include "vast/fwd.hpp"
#include "vast/time.hpp"
#include "vast/uuid.hpp"

namespace vast {

/// Maps events to horizontal partitions of the ::index.
class meta_index {
public:
  // -- member types -----------------------------------------------------------

  /// A closed interval.
  struct interval {
    interval();
    interval(timestamp from, timestamp to);

    timestamp from;
    timestamp to;
  };

  /// Per-partition summary statistics.
  struct partition_synopsis {
    interval range;
  };

  using map_type = std::unordered_map<uuid, partition_synopsis>;

  using const_iterator = map_type::const_iterator;

  // -- properties -------------------------------------------------------------

  /// @returns the synopsis for a partition if present, returns `none`
  ///          otherwise.
  caf::optional<partition_synopsis> operator[](const uuid& partition) const;

  void add(const uuid& partition, const const_table_slice_handle& slice);

  /// Retrieves the list of partition IDs for a given expression.
  std::vector<uuid> lookup(const expression& expr) const;

  size_t size() const noexcept {
    return partitions_.size();
  }

  const_iterator begin() const noexcept {
    return partitions_.begin();
  }

  const_iterator end() const noexcept {
    return partitions_.end();
  }

  const map_type& partitions() const noexcept {
    return partitions_;
  }

  // -- inspection -------------------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, interval& i) {
    return f(caf::meta::type_name("interval"), i.from, i.to);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, partition_synopsis& ps) {
    return f(ps.range);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, meta_index& pi) {
    return f(pi.partitions_);
  }

private:
  // -- member variables -------------------------------------------------------

  map_type partitions_;
};

// -- related free functions ---------------------------------------------------

/// @relates meta_index::interval
bool operator==(const meta_index::interval&, const meta_index::interval&);

/// @relates meta_index::interval
inline bool operator!=(const meta_index::interval& x,
                       const meta_index::interval& y) {
  return !(x == y);
}

} // namespace vast
