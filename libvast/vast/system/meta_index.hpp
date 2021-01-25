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

#include "vast/fbs/index.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/ids.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis.hpp"
#include "vast/time_synopsis.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <caf/fwd.hpp>
#include <caf/settings.hpp>

#include <flatbuffers/flatbuffers.h>

#include <functional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace vast::system {

// Forward declaration to be able to `friend` this function.
caf::expected<flatbuffers::Offset<fbs::Partition>>
pack(flatbuffers::FlatBufferBuilder& builder,
     const system::active_partition_state& x);

/// The meta index is the first data structure that queries hit. The result
/// represents a list of candidate partition IDs that may contain the desired
/// data. The meta index may return false positives but never false negatives.
class meta_index {
public:
  /// Adds new synopses for a partition in bulk. Used when
  /// re-building the meta index state at startup.
  void merge(const uuid& partition, partition_synopsis&&);

  /// Returns the partition synopsis for a specific partition.
  /// Note that most callers will prefer to use `lookup()` instead.
  /// @pre `partition` must be a valid key for this meta index.
  partition_synopsis& at(const uuid& partition);

  /// Erase this partition from the meta index.
  void erase(const uuid& partition);

  /// Retrieves the list of candidate partition IDs for a given expression.
  /// @param expr The expression to lookup.
  /// @returns A vector of UUIDs representing candidate partitions.
  std::vector<uuid> lookup(const expression& expr) const;

  /// @returns A best-effort estimate of the amount of memory used for this meta
  /// index (in bytes).
  size_t memusage() const;

  // -- concepts ---------------------------------------------------------------

  // Allow debug printing meta_index instances.
  template <class Inspector>
  friend auto inspect(Inspector& f, meta_index& x) {
    return f(x.synopses_);
  }

  // Allow the partition to directly serialize the relevant synopses.
  friend caf::expected<flatbuffers::Offset<fbs::Partition>>
  pack(flatbuffers::FlatBufferBuilder& builder,
       const system::active_partition_state& x);

private:
  /// Maps a partition ID to the synopses for that partition.
  std::unordered_map<uuid, partition_synopsis> synopses_;
};

} // namespace vast::system
