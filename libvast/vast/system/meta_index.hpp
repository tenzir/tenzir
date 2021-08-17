//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/detail/flat_map.hpp"
#include "vast/fbs/index.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/ids.hpp"
#include "vast/legacy_type.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/synopsis.hpp"
#include "vast/system/actors.hpp"
#include "vast/time_synopsis.hpp"
#include "vast/uuid.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <map>
#include <string>
#include <vector>

namespace vast::system {

/// The state of the META INDEX actor.
struct meta_index_state {
public:
  // -- constructor ------------------------------------------------------------

  meta_index_state() = default;

  // -- concepts ---------------------------------------------------------------

  constexpr static auto name = "meta-index";

  // -- utility functions ------------------------------------------------------

  /// Adds new synopses for a partition in bulk. Used when
  /// re-building the meta index state at startup.
  void create_from(std::map<uuid, partition_synopsis>&&);

  /// Add a new partition synopsis.
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
  [[nodiscard]] std::vector<uuid> lookup(const expression& expr) const;

  [[nodiscard]] std::vector<uuid> lookup_impl(const expression& expr) const;

  /// @returns A best-effort estimate of the amount of memory used for this meta
  /// index (in bytes).
  [[nodiscard]] size_t memusage() const;

  // -- data members -----------------------------------------------------------

  /// A pointer to the parent actor.
  meta_index_actor::pointer self = {};

  /// Maps a partition ID to the synopses for that partition.
  // We mainly iterate over the whole map and return a sorted set, for which
  // the `flat_map` proves to be much faster than `std::{unordered_,}set`.
  // See also ae9dbed.
  detail::flat_map<uuid, partition_synopsis> synopses = {};
};

/// The META INDEX is the first index actor that queries hit. The result
/// represents a list of candidate partition IDs that may contain the desired
/// data. The META INDEX may return false positives but never false negatives.
/// @param self The actor handle.
meta_index_actor::behavior_type
meta_index(meta_index_actor::stateful_pointer<meta_index_state> self);

} // namespace vast::system
