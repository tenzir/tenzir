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

#include <functional>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include <caf/fwd.hpp>

#include "vast/fwd.hpp"
#include "vast/synopsis.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

namespace vast {

/// The meta index is the first data structure that queries hit. The result
/// represents a list of candidate partition IDs that may contain the desired
/// data. The meta index may return false positives but never false negatives.
class meta_index {
public:
  // -- initialization ---------------------------------------------------------

  meta_index();

  // -- API --------------------------------------------------------------------

  /// Adds all data from a table slice belonging to a given partition to the
  /// index.
  /// @param slice The table slice to extract data from.
  /// @param partition The partition ID that *slice* belongs to.
  void add(const uuid& partition, const table_slice& slice);

  /// Retrieves the list of candidate partition IDs for a given expression.
  /// @param expr The expression to lookup.
  /// @returns A vector of UUIDs representing candidate partitions.
  std::vector<uuid> lookup(const expression& expr) const;

  /// Replaces the synopsis factory.
  /// @param f The factory to use.
  void factory(synopsis_factory f);

  // -- concepts ---------------------------------------------------------------

  template <class Inspector>
  friend auto inspect(Inspector& f, meta_index& x) {
    return f(x.partition_synopses_);
  }

private:
  // Synopsis structures for a givn layout.
  using table_synopsis = std::vector<synopsis_ptr>;

  /// Contains synopses per table layout.
  using partition_synopsis = std::unordered_map<record_type, table_synopsis>;

  /// Layouts for which we cannot generate a synopsis structure.
  std::unordered_set<record_type> blacklisted_layouts_;

  /// Maps a partition ID to the synopses for that partition.
  std::unordered_map<uuid, partition_synopsis> partition_synopses_;

  /// The factory function to construct a synopsis structure for a type.
  synopsis_factory make_synopsis_;
};

/// Tries to set a new synopsis factory from an actor system.
/// @param x The meta index instance.
/// @param sys The actor system.
/// @returns `true` iff *sys* contains a synopsis factory.
/// @relates meta_index
bool set_synopsis_factory(meta_index& x, caf::actor_system& sys);

} // namespace vast
