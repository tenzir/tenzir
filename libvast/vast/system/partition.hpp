//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/aliases.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/ids.hpp"
#include "vast/partition_synopsis.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/query.hpp"
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
#include <unordered_map>
#include <vector>

namespace vast::system {

/// Helper class used to route table slice columns to the correct indexer
/// in the CAF stream stage.
struct partition_selector {
  bool operator()(const qualified_record_field& filter,
                  const table_slice_column& column) const;
};

/// The state of the ACTIVE PARTITION actor.
struct active_partition_state {
  // -- member types -----------------------------------------------------------

  using partition_stream_stage_ptr = caf::stream_stage_ptr<
    table_slice,
    caf::broadcast_downstream_manager<
      table_slice_column, vast::qualified_record_field, partition_selector>>;

  // -- utility functions ------------------------------------------------------

  active_indexer_actor indexer_at(size_t position) const;

  void add_flush_listener(flush_listener_actor listener);

  void notify_flush_listeners();

  // -- data members -----------------------------------------------------------

  /// Pointer to the parent actor.
  active_partition_actor::pointer self = nullptr;

  /// Uniquely identifies this partition.
  uuid id;

  /// The streaming stage.
  partition_stream_stage_ptr stage;

  /// Tracks whether we already received at least one table slice.
  bool streaming_initiated;

  /// The combined type of all columns of this partition
  record_type combined_layout;

  /// Maps qualified fields to indexer actors.
  //  TODO: Should we use the tsl map here for heterogenous key lookup?
  detail::stable_map<qualified_record_field, active_indexer_actor> indexers;

  /// Maps type names to IDs. Used the answer #type queries.
  std::unordered_map<std::string, ids> type_ids;

  /// Partition synopsis for this partition. This is built up in parallel
  /// to the one in the index, so it can be shrinked and serialized into
  /// a `Partition` flatbuffer upon completion of this partition. Will be
  /// sent back to the partition after persisting to minimize memory footprint
  /// of the meta index.
  /// Semantically this should be a unique_ptr, but caf requires message
  /// types to be copy-constructible.
  std::shared_ptr<partition_synopsis> synopsis;

  /// Options to be used when adding events to the partition_synopsis.
  caf::settings synopsis_opts;

  /// A readable name for this partition
  std::string name;

  /// The first ID in the partition.
  vast::id offset;

  /// The number of events in the partition.
  size_t events;

  /// Actor handle of the filesystem actor.
  filesystem_actor filesystem;

  /// Promise that gets satisfied after the partition state was serialized
  /// and written to disk.
  caf::typed_response_promise<std::shared_ptr<partition_synopsis>>
    persistence_promise;

  /// Path where the index state is written.
  std::optional<std::filesystem::path> persist_path;

  /// Path where the partition synopsis is written.
  std::optional<std::filesystem::path> synopsis_path;

  /// Counts how many indexers have already responded to the `snapshot` atom
  /// with a serialized chunk.
  size_t persisted_indexers;

  /// The store to retrieve the data from. Either the legacy global archive or a
  /// local component that holds the data for this partition.
  store_actor store;

  /// Temporary storage for the serialized indexers of this partition, before
  /// they get written into the flatbuffer.
  std::map<caf::actor_id, vast::chunk_ptr> chunks;

  /// A once_flag for things that need to be done only once at shutdown.
  std::once_flag shutdown_once;

  // Vector of flush listeners.
  std::vector<flush_listener_actor> flush_listeners;
};

// TODO: Split this into a `static data` part that can be mmaped
// straight from disk, and an actor-related part that contains the
// former. In the ideal case, we want to be able to use the on-disk
// state without any intermediate deserialization step,
// like yandex::mms or cap'n proto.
struct passive_partition_state {
  // -- member types -----------------------------------------------------------

  using recovered_indexer = std::pair<qualified_record_field, value_index_ptr>;

  // -- utility functions ------------------------------------------------------

  indexer_actor indexer_at(size_t position) const;

  // -- data members -----------------------------------------------------------

  /// Pointer to the parent actor.
  partition_actor::pointer self = nullptr;

  /// Uniquely identifies this partition.
  uuid id;

  /// The combined type of all columns of this partition
  record_type combined_layout;

  /// Maps type names to ids. Used the answer #type queries.
  std::unordered_map<std::string, ids> type_ids;

  /// A readable name for this partition
  std::string name;

  /// The first ID in the partition.
  size_t offset;

  /// The number of events in the partition.
  size_t events;

  /// The raw memory of the partition, used to spawn indexers on demand.
  chunk_ptr partition_chunk;

  /// Stores a list of expressions that could not be answered immediately.
  std::vector<std::tuple<query, caf::weak_actor_ptr,
                         caf::typed_response_promise<atom::done>>>
    deferred_evaluations;

  /// The store to retrieve the data from. Either the legacy global archive or a
  /// local component that holds the data for this partition.
  store_actor store;

  /// A typed view into the `partition_chunk`.
  const fbs::partition::v0* flatbuffer;

  /// Maps qualified fields to indexer actors. This is mutable since
  /// indexers are spawned lazily on first access.
  mutable std::vector<indexer_actor> indexers;
};

// -- flatbuffers --------------------------------------------------------------

caf::expected<flatbuffers::Offset<fbs::Partition>>
pack(flatbuffers::FlatBufferBuilder& builder, const active_partition_state& x);

caf::error unpack(const fbs::partition::v0& x, passive_partition_state& y);

caf::error unpack(const fbs::partition::v0& x, partition_synopsis& y);

// -- behavior -----------------------------------------------------------------

/// Spawns a partition.
/// @param self The partition actor.
/// @param id The UUID of this partition.
/// @param filesystem The actor handle of the filesystem actor.
/// @param index_opts Settings that are forwarded when creating indexers.
/// @param synopsis_opts Settings that are forwarded when creating synopses.
/// @param store The store to retrieve the events from.
active_partition_actor::behavior_type active_partition(
  active_partition_actor::stateful_pointer<active_partition_state> self,
  uuid id, filesystem_actor filesystem, caf::settings index_opts,
  caf::settings synopsis_opts, store_actor store);

/// Spawns a read-only partition.
/// @param self The partition actor.
/// @param id The UUID of this partition.
/// @param filesystem The actor handle of the filesystem actor.
/// @param path The path where the partition flatbuffer can be found.
/// @param store The store to retrieve the events from.
partition_actor::behavior_type passive_partition(
  partition_actor::stateful_pointer<passive_partition_state> self, uuid id,
  filesystem_actor filesystem, const std::filesystem::path& path,
  store_actor store);

} // namespace vast::system
