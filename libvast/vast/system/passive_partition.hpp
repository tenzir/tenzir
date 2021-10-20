//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/ids.hpp"
#include "vast/legacy_type.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/query.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/evaluator.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/uuid.hpp"
#include "vast/value_index.hpp"

#include <caf/optional.hpp>
#include <caf/stream_slot.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <span>
#include <unordered_map>
#include <vector>

namespace vast::system {

// TODO: Split this into a `static data` part that can be mmaped
// straight from disk, and an actor-related part that contains the
// former, similar to `active_partition_state`.
struct passive_partition_state {
  // -- constructor ------------------------------------------------------------

  passive_partition_state() = default;

  // -- member types -----------------------------------------------------------

  using recovered_indexer = std::pair<qualified_record_field, value_index_ptr>;

  // -- utility functions ------------------------------------------------------

  indexer_actor indexer_at(size_t position) const;

  const vast::legacy_record_type& combined_layout() const;

  const std::unordered_map<std::string, ids>& type_ids() const;

  // -- data members -----------------------------------------------------------

  /// Pointer to the parent actor.
  partition_actor::pointer self = nullptr;

  /// Path of the underlying file for this partition.
  std::filesystem::path path;

  /// Actor handle of the legacy archive.
  store_actor archive = {};

  /// Uniquely identifies this partition.
  uuid id = {};

  /// The combined type of all columns of this partition.
  legacy_record_type combined_layout_ = {};

  /// Maps type names to ids. Used the answer #type queries.
  std::unordered_map<std::string, ids> type_ids_ = {};

  /// A readable name for this partition.
  std::string name = {};

  /// The first ID in the partition.
  size_t offset = {};

  /// The number of events in the partition.
  size_t events = {};

  /// The store type as found in the flatbuffer.
  std::string store_id = {};

  /// The store header as found in the flatbuffer.
  std::span<const std::byte> store_header = {};

  /// The raw memory of the partition, used to spawn indexers on demand.
  chunk_ptr partition_chunk = {};

  /// Stores a list of expressions that could not be answered immediately.
  std::vector<std::tuple<query, caf::typed_response_promise<atom::done>>>
    deferred_evaluations = {};

  /// Actor handle of the filesystem.
  filesystem_actor filesystem = {};

  /// The store to retrieve the data from. Either the legacy global archive or a
  /// local component that holds the data for this partition.
  store_actor store = {};

  /// Actor handle of the node.
  node_actor::pointer node = {};

  /// A typed view into the `partition_chunk`.
  const fbs::partition::v0* flatbuffer = {};

  /// Maps qualified fields to indexer actors. This is mutable since
  /// indexers are spawned lazily on first access.
  mutable std::vector<indexer_actor> indexers = {};
};

// -- flatbuffers --------------------------------------------------------------

[[nodiscard]] caf::error
unpack(const fbs::partition::v0& x, passive_partition_state& y);

[[nodiscard]] caf::error
unpack(const fbs::partition::v0& x, partition_synopsis& y);

// -- behavior -----------------------------------------------------------------

/// Spawns a read-only partition.
/// @param self The partition actor.
/// @param id The UUID of this partition.
/// @param filesystem The actor handle of the filesystem actor.
/// @param path The path where the partition flatbuffer can be found.
/// @param store The store to retrieve the events from.
partition_actor::behavior_type passive_partition(
  partition_actor::stateful_pointer<passive_partition_state> self, uuid id,
  store_actor archive, filesystem_actor filesystem,
  const std::filesystem::path& path);

} // namespace vast::system
