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
#include "tenzir/aliases.hpp"
#include "tenzir/evaluator.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/index_config.hpp"
#include "tenzir/indexer.hpp"
#include "tenzir/instrumentation.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/qualified_record_field.hpp"
#include "tenzir/query_context.hpp"
#include "tenzir/type.hpp"
#include "tenzir/uuid.hpp"
#include "tenzir/value_index.hpp"

#include <caf/optional.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <span>
#include <unordered_map>
#include <vector>

namespace tenzir {

/// The state of the ACTIVE PARTITION actor.
struct active_partition_state {
  // -- constructor ------------------------------------------------------------

  active_partition_state() = default;

  // -- member types -----------------------------------------------------------

  /// Contains all the data necessary to create a partition flatbuffer.
  struct serialization_data {
    /// Uniquely identifies this partition.
    tenzir::uuid id = {};

    /// The number of events in the partition.
    size_t events = {};

    /// The name of the store backend
    std::string store_id = {};

    /// Opaque blob that is passed to the store backend on reading.
    chunk_ptr store_header = {};

    // A handle to the store builder.
    // Only used by the partition transformer.
    store_builder_actor builder = {};

    /// Maps type names to IDs. Used the answer #schema queries.
    std::unordered_map<std::string, ids> type_ids = {};

    /// Partition synopsis for this partition. This is built up in parallel
    /// to the one in the index, so it can be shrinked and serialized into
    /// a `Partition` flatbuffer upon completion of this partition. Will be
    /// sent back to the partition after persisting to minimize memory footprint
    /// of the catalog.
    partition_synopsis_ptr synopsis = {};

    /// A mapping from qualified field name to serialized indexer state
    /// for each indexer in the partition.
    std::vector<std::pair<std::string, chunk_ptr>> indexer_chunks = {};
  };

  // -- inbound path -----------------------------------------------------------

  void handle_slice(table_slice slice);

  // -- data members -----------------------------------------------------------

  /// Pointer to the parent actor.
  active_partition_actor::pointer self = nullptr;

  /// The data that will end up on disk in the partition flatbuffer.
  serialization_data data;

  /// Options to be used when adding events to the partition_synopsis.
  uint64_t partition_capacity = 0ull;
  index_config synopsis_index_config = {};

  /// A readable name for this partition.
  static constexpr auto name = "active-partition";

  /// Actor handle of the filesystem.
  filesystem_actor filesystem = {};

  /// Promise that gets satisfied after the partition state was serialized
  /// and written to disk.
  caf::typed_response_promise<partition_synopsis_ptr> persistence_promise = {};

  /// Path where the index state is written.
  std::optional<std::filesystem::path> persist_path = {};

  /// Path where the partition synopsis is written.
  std::optional<std::filesystem::path> synopsis_path = {};

  /// The store backend.
  const store_actor_plugin* store_plugin = {};

  /// The store builder.
  store_builder_actor store_builder = {};

  /// Access info for the finished store.
  std::optional<resource> store_file = {};

  /// Temporary storage for the serialized indexers of this partition, before
  /// they get written into the flatbuffer.
  std::map<caf::actor_id, tenzir::chunk_ptr> chunks = {};

  /// A once_flag for things that need to be done only once at shutdown.
  std::once_flag shutdown_once = {};

  // Vector of flush listeners.
  std::vector<flush_listener_actor> flush_listeners = {};

  // Taxonomies for resolving expressions during a query.
  std::shared_ptr<tenzir::taxonomies> taxonomies = {};
};

// -- flatbuffers --------------------------------------------------------------

// The resulting chunk will start with either a `tenzir::fbs::Partition` or a
// `tenzir::fbs::SegmentedFileHeader`.
caf::expected<tenzir::chunk_ptr>
pack_full(const active_partition_state::serialization_data& x,
          const record_type& combined_schema);

// -- behavior -----------------------------------------------------------------

/// Spawns a partition.
/// @param self The partition actor.
/// @param schema The schema of this partition.
/// @param id The UUID of this partition.
/// @param filesystem The actor handle of the filesystem.
/// @param index_opts Settings that are forwarded when creating indexers.
/// @param index_config The meta-index configuration of the false-positives
/// @param store A pointer to the store impplementation.
/// @param taxonomies The taxonomies for resolving expressions during a query.
/// rates for the types and fields.
// TODO: Bundle store, store_id and store_header in a single struct
active_partition_actor::behavior_type active_partition(
  active_partition_actor::stateful_pointer<active_partition_state> self,
  type schema, uuid id, filesystem_actor filesystem, caf::settings index_opts,
  const index_config& synopsis_opts, const store_actor_plugin* store_plugin,
  std::shared_ptr<tenzir::taxonomies> taxonomies);

} // namespace tenzir
