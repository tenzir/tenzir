//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/fbs/flatbuffer_container.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/type.hpp"
#include "tenzir/uuid.hpp"
#include "tenzir/value_index.hpp"

#include <caf/optional.hpp>
#include <caf/stream_slot.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <span>
#include <unordered_map>
#include <vector>

namespace tenzir {

// TODO: Split this into a `static data` part that can be mmaped
// straight from disk, and an actor-related part that contains the
// former, similar to `active_partition_state`.
struct passive_partition_state {
  // -- constructor ------------------------------------------------------------

  passive_partition_state() = default;

  caf::error initialize_from_chunk(const tenzir::chunk_ptr&);

  // -- member types -----------------------------------------------------------

  using recovered_indexer = std::pair<qualified_record_field, value_index_ptr>;

  // -- utility functions ------------------------------------------------------

  indexer_actor indexer_at(size_t position) const;

  const std::optional<tenzir::record_type>& combined_schema() const;

  const std::unordered_map<std::string, ids>& type_ids() const;

  // -- data members -----------------------------------------------------------

  /// Pointer to the parent actor.
  partition_actor::pointer self = nullptr;

  /// Path of the underlying file for this partition.
  std::filesystem::path path;

  /// Uniquely identifies this partition.
  uuid id = {};

  /// The combined type of all columns of this partition.
  std::optional<record_type> combined_schema_ = {};

  /// Maps type names to ids. Used the answer #schema queries.
  std::unordered_map<std::string, ids> type_ids_ = {};

  /// A readable name for this partition.
  static constexpr auto name = "passive-partition";

  /// The number of events in the partition.
  size_t events = {};

  /// The store type as found in the flatbuffer.
  std::string store_id = {};

  /// The store header as found in the flatbuffer.
  std::span<const std::byte> store_header = {};

  /// The raw memory of the partition, used to spawn indexers on demand.
  chunk_ptr partition_chunk = {};

  /// Stores a list of expressions that could not be answered immediately.
  std::vector<std::tuple<query_context, caf::typed_response_promise<uint64_t>>>
    deferred_evaluations = {};

  /// Stores a list of erasures that could not be answered immediately.
  std::vector<caf::typed_response_promise<atom::done>> deferred_erasures = {};

  /// Actor handle of the filesystem.
  filesystem_actor filesystem = {};

  /// The store to retrieve the data from.
  store_actor store = {};

  /// Actor handle of the node.
  node_actor::pointer node = {};

  /// A typed view into the `partition_chunk`.
  const fbs::partition::LegacyPartition* flatbuffer = {};

  /// The flatbuffer container holding the index data
  std::optional<fbs::flatbuffer_container> container = {};

  /// Maps qualified fields to indexer actors. This is mutable since
  /// indexers are spawned lazily on first access.
  mutable std::vector<indexer_actor> indexers = {};
};

// -- flatbuffers --------------------------------------------------------------

[[nodiscard]] value_index_ptr
unpack_value_index(const fbs::value_index::detail::LegacyValueIndex& index_fbs,
                   const fbs::flatbuffer_container& container);

[[nodiscard]] caf::error
unpack(const fbs::partition::LegacyPartition&, passive_partition_state&);

[[nodiscard]] caf::error
unpack(const fbs::partition::LegacyPartition&, partition_synopsis&);

/// Get various parts of a passive partition from a chunk containing a partition
/// file. These functions hide the differences of the underlying file formats
/// used by different Tenzir versions. They are also a stop-gap until we
/// introduce a dedicated class to wrap a partition flatbuffer.
struct partition_chunk {
  static caf::expected<const tenzir::fbs::Partition*>
    get_flatbuffer(tenzir::chunk_ptr);
};

// -- behavior -----------------------------------------------------------------

/// Spawns a read-only partition.
/// @param self The partition actor.
/// @param id The UUID of this partition.
/// @param filesystem The actor handle of the filesystem actor.
/// @param path The path where the partition flatbuffer can be found.
partition_actor::behavior_type passive_partition(
  partition_actor::stateful_pointer<passive_partition_state> self, uuid id,
  filesystem_actor filesystem, const std::filesystem::path& path);

} // namespace tenzir
