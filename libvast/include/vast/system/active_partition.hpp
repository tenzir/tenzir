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
#include "vast/index_config.hpp"
#include "vast/partition_sketch_builder.hpp"
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
#include <span>
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
  // -- constructor ------------------------------------------------------------

  active_partition_state() = default;

  // -- member types -----------------------------------------------------------

  using partition_stream_stage_ptr = caf::stream_stage_ptr<
    table_slice,
    caf::broadcast_downstream_manager<
      table_slice_column, vast::qualified_record_field, partition_selector>>;

  /// Contains all the data necessary to create a partition flatbuffer.
  struct serialization_data {
    /// Uniquely identifies this partition.
    vast::uuid id = {};

    /// The first ID in the partition.
    vast::id offset = {};

    /// The number of events in the partition.
    size_t events = {};

    /// The name of the store backend
    std::string store_id = {};

    /// Opaque blob that is passed to the store backend on reading.
    chunk_ptr store_header = {};

    /// Maps type names to IDs. Used the answer #type queries.
    std::unordered_map<std::string, ids> type_ids = {};

    /// Partition synopsis for this partition.
    partition_synopsis_ptr synopsis = {};

    /// A mapping from qualified field name to serialized indexer state
    /// for each indexer in the partition.
    std::vector<std::pair<std::string, chunk_ptr>> indexer_chunks = {};
  };

  // -- utility functions ------------------------------------------------------

  active_indexer_actor indexer_at(size_t position) const;

  void add_flush_listener(flush_listener_actor listener);

  void notify_flush_listeners();

  std::optional<vast::record_type> combined_layout() const;

  const std::unordered_map<std::string, ids>& type_ids() const;

  // -- data members -----------------------------------------------------------

  /// Pointer to the parent actor.
  active_partition_actor::pointer self = nullptr;

  /// The data that will end up on disk in the partition flatbuffer.
  serialization_data data = {};

  /// Whether this partition should use sketches or legacy synopses
  /// for its probabilistic summary data structures.
  bool use_sketches = {};

  // The import time range of this partition.
  time min_import_time_ = time::clock::time_point::max();
  time max_import_time_ = time::clock::time_point::min();

  /// The sketch builder.
  // TODO: Add a default constructor to the sketch builder
  // so we can store it in the state directly.
  caf::expected<partition_sketch_builder> sketch_builder = caf::no_error;

  /// We (ab)use a separate partition synopsis as builder when configured
  /// to use legacy synopsis.
  partition_synopsis_ptr legacy_synopsis_builder = nullptr;

  /// The streaming stage.
  partition_stream_stage_ptr stage = {};

  /// Tracks whether we already received at least one table slice.
  bool streaming_initiated = {};

  /// Options to be used when adding events to the partition_synopsis.
  uint64_t partition_capacity = 0ull;

  /// Indexing-related user configuration.
  index_config synopsis_index_config = {};

  /// A readable name for this partition.
  std::string name = {};

  /// Actor handle of the accountant.
  accountant_actor accountant = {};

  /// Actor handle of the filesystem.
  filesystem_actor filesystem = {};

  /// Promise that gets satisfied after the partition state was serialized
  /// and written to disk.
  caf::typed_response_promise<partition_synopsis_ptr> persistence_promise = {};

  /// Path where the index state is written.
  std::optional<std::filesystem::path> persist_path = {};

  /// Path where the partition synopsis is written.
  std::optional<std::filesystem::path> synopsis_path = {};

  /// Maps qualified fields to indexer actors.
  //  TODO: Should we use the tsl map here for heterogeneous key lookup?
  detail::stable_map<qualified_record_field, active_indexer_actor> indexers
    = {};

  /// Counts how many indexers have already responded to the `snapshot` atom
  /// with a serialized chunk.
  size_t persisted_indexers = {};

  /// The store to retrieve the data from. Either the legacy global archive or a
  /// local component that holds the data for this partition.
  store_actor store = {};

  /// Temporary storage for the serialized indexers of this partition, before
  /// they get written into the flatbuffer.
  std::map<caf::actor_id, vast::chunk_ptr> chunks = {};

  /// A once_flag for things that need to be done only once at shutdown.
  std::once_flag shutdown_once = {};

  // Vector of flush listeners.
  std::vector<flush_listener_actor> flush_listeners = {};
};

// -- flatbuffers --------------------------------------------------------------

caf::expected<flatbuffers::Offset<fbs::Partition>>
pack(flatbuffers::FlatBufferBuilder& builder,
     const active_partition_state::serialization_data& x,
     const record_type& combined_layout);

// -- behavior -----------------------------------------------------------------

/// Spawns a partition.
/// @param self The partition actor.
/// @param id The UUID of this partition.
/// @param partition_layout The layout of this partition. Must be a named
/// record_type.
/// @param accountant The actor handle of the accountant.
/// @param filesystem The actor handle of the filesystem.
/// @param index_opts Settings that are forwarded when creating indexers.
/// @param store The store to retrieve the events from.
/// @param store_id The name of the store backend that should be stored
///                      on disk.
/// @param store_header A binary blob that allows reconstructing the store
///                     plugin when reading this partition from disk.
/// @param index_config The meta-index configuration of the false-positives
/// rates for the types and fields.
// TODO: Bundle store, store_id and store_header in a single struct
active_partition_actor::behavior_type active_partition(
  active_partition_actor::stateful_pointer<active_partition_state> self,
  uuid id, type partition_layout, accountant_actor accountant,
  filesystem_actor filesystem, caf::settings index_opts,
  const index_config& synopsis_opts, store_actor store, std::string store_id,
  chunk_ptr store_header);

} // namespace vast::system
