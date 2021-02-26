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
#include "vast/system/actors.hpp"
#include "vast/time_synopsis.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <string>
#include <unordered_map>
#include <vector>

namespace vast::system {

/// The state of the META INDEX actor.
struct meta_index_state {
public:
  // -- concepts ---------------------------------------------------------------

  constexpr static auto name = "meta-index";

  // -- utility functions ------------------------------------------------------

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

  // -- data members -----------------------------------------------------------

  /// A pointer to the parent actor.
  meta_index_actor::pointer self;

  /// Maps a partition ID to the synopses for that partition.
  std::unordered_map<uuid, partition_synopsis> synopses;
};

/// The META INDEX is the first index actor that queries hit. The result
/// represents a list of candidate partition IDs that may contain the desired
/// data. The META INDEX may return false positives but never false negatives.
/// @param self The actor handle.
meta_index_actor::behavior_type
meta_index(meta_index_actor::stateful_pointer<meta_index_state> self);

} // namespace vast::system
