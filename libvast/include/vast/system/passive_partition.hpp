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
#include "vast/fbs/flatbuffer_container.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/segmented_file.hpp"
#include "vast/ids.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/query_context.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/evaluator.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/type.hpp"
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

  caf::error initialize_from_chunk(const vast::chunk_ptr&);

  // -- member types -----------------------------------------------------------

  using recovered_indexer = std::pair<qualified_record_field, value_index_ptr>;

  // -- utility functions ------------------------------------------------------

  indexer_actor indexer_at(size_t position) const;

  const std::optional<vast::record_type>& combined_layout() const;

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
  std::optional<record_type> combined_layout_ = {};

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

  /// Actor handle of the accountant.
  accountant_actor accountant = {};

  /// Actor handle of the filesystem.
  filesystem_actor filesystem = {};

  /// The store to retrieve the data from. Either the legacy global archive or a
  /// local component that holds the data for this partition.
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

[[nodiscard]] caf::error
unpack(const fbs::partition::LegacyPartition&, passive_partition_state&);

[[nodiscard]] caf::error
unpack(const fbs::partition::LegacyPartition&, partition_synopsis&);

/// Get various parts of a passive partition from a chunk containing a partition
/// file. These functions hide the differences of the underlying file formats
/// used by different VAST versions. They are also a stop-gap until we introduce
/// a dedicated class to wrap a partition flatbuffer.
struct partition_chunk {
  static caf::expected<index_statistics> get_statistics(vast::chunk_ptr);

  static caf::expected<const vast::fbs::Partition*>
    get_flatbuffer(vast::chunk_ptr);
};

// -- behavior -----------------------------------------------------------------

/// Spawns a read-only partition.
/// @param self The partition actor.
/// @param id The UUID of this partition.
/// @param accountant the accountant to send metrics to.
/// @param archive The legacy archive to retrieve the events from.
/// @param filesystem The actor handle of the filesystem actor.
/// @param path The path where the partition flatbuffer can be found.
partition_actor::behavior_type passive_partition(
  partition_actor::stateful_pointer<passive_partition_state> self, uuid id,
  accountant_actor accountant, store_actor archive, filesystem_actor filesystem,
  const std::filesystem::path& path);

} // namespace vast::system
