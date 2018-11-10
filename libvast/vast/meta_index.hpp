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

#include <caf/atom.hpp>
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
  /// @param factory_id The system-wide ID for `f`.
  /// @param f The synopsis factory to use.
  /// @pre `f` is registered in the runtime settings unter the key
  ///      `factory_id`
  void factory(caf::atom_value factory_id, synopsis_factory f);

  // -- concepts ---------------------------------------------------------------

  friend caf::error inspect(caf::serializer&, const meta_index&);

  friend caf::error inspect(caf::deserializer&, meta_index&);

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

  /// The implementation ID for objects created by `make_synopsis_`.
  caf::atom_value factory_id_;
};

} // namespace vast
