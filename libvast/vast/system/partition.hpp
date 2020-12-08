/******************************************************************************

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

#include "vast/aliases.hpp"
#include "vast/expression.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/meta_index.hpp"
#include "vast/path.hpp"
#include "vast/qualified_record_field.hpp"
#include "vast/system/evaluator.hpp"
#include "vast/system/filesystem.hpp"
#include "vast/system/indexer.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"
#include "vast/value_index.hpp"

#include <caf/fwd.hpp>
#include <caf/optional.hpp>
#include <caf/stream_slot.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <unordered_map>

namespace vast::system {

/// Helper class used to route table slice columns to the correct indexer
/// in the CAF stream stage.
struct partition_selector {
  bool operator()(const vast::qualified_record_field& filter,
                  const table_slice_column& x) const;
};

// clang-format off
using partition_actor = caf::typed_actor<
  caf::replies_to<caf::stream<table_slice>>
    ::with<caf::inbound_stream_slot<table_slice>>,
  caf::replies_to<atom::persist, path, caf::actor>
    ::with<atom::ok>,
  caf::reacts_to<atom::persist, atom::resume>,
  caf::reacts_to<chunk_ptr>,
  caf::replies_to<expression>::with<evaluation_triples>
>;
// clang-format on

/// The state of the active partition actor.
struct active_partition_state {
  using partition_stream_stage_ptr = caf::stream_stage_ptr<
    table_slice,
    caf::broadcast_downstream_manager<
      table_slice_column, vast::qualified_record_field, partition_selector>>;

  indexer_actor indexer_at(size_t position) const;

  /// Data Members

  /// Pointer to the parent actor.
  partition_actor::pointer self;

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
  detail::stable_map<qualified_record_field, indexer_actor> indexers;

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

  /// Actor handle of the index actor.
  caf::actor index_actor;

  /// Actor handle of the filesystem actor.
  filesystem_type fs_actor;

  /// Promise that gets satisfied when the partition state was serialized
  /// and written to disk.
  caf::typed_response_promise<atom::ok> persistence_promise;

  /// Path where the index state is written.
  std::optional<path> persist_path;

  /// Counts how many indexers have already responded to the `snapshot` atom
  /// with a serialized chunk.
  size_t persisted_indexers;

  /// Temporary storage for the serialized indexers of this partition, before
  /// they get written into the flatbuffer.
  std::map<caf::actor_id, vast::chunk_ptr> chunks;

  /// A once_flag for things that need to be done only once at shutdown.
  std::once_flag shutdown_once;
};

// TODO: Split this into a `static data` part that can be mmaped
// straight from disk, and an actor-related part that contains the
// former. In the ideal case, we want to be able to use the on-disk
// state without any intermediate deserialization step,
// like yandex::mms or cap'n proto.
struct passive_partition_state {
  using recovered_indexer = std::pair<qualified_record_field, value_index_ptr>;

  indexer_actor indexer_at(size_t position) const;

  /// Pointer to the parent actor.
  partition_actor::pointer self;

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
  vast::chunk_ptr partition_chunk;

  /// Stores a list of expressions that could not be answered immediately.
  std::vector<
    std::pair<expression, caf::typed_response_promise<evaluation_triples>>>
    deferred_evaluations;

  /// A typed view into the `partition_chunk`.
  const fbs::partition::v0* flatbuffer;

  /// Maps qualified fields to indexer actors. This is mutable since
  /// indexers are spawned lazily on first access.
  mutable std::vector<indexer_actor> indexers;
};

// Flatbuffer support

caf::expected<flatbuffers::Offset<fbs::Partition>>
pack(flatbuffers::FlatBufferBuilder& builder, const active_partition_state& x);

caf::error unpack(const fbs::partition::v0& x, passive_partition_state& y);

caf::error unpack(const fbs::partition::v0& x, partition_synopsis& y);

/// Spawns a partition.
/// @param self The partition actor.
/// @param id The UUID of this partition.
/// @param fs The actor handle of the filesystem actor.
/// @param index_opts Settings that are forwarded when creating indexers.
/// @param synopsis_opts Settings that are forwarded when creating synopses.
partition_actor::behavior_type
active_partition(partition_actor::stateful_pointer<active_partition_state> self,
                 uuid id, filesystem_type fs, caf::settings index_opts,
                 caf::settings synopsis_opts);

/// Spawns a read-only partition.
/// @param self The partition actor.
/// @param id The UUID of this partition.
/// @param fs The actor handle of the filesystem actor.
/// @param path The path where the partition flatbuffer can be found.
partition_actor::behavior_type passive_partition(
  partition_actor::stateful_pointer<passive_partition_state> self, uuid id,
  filesystem_type fs, vast::path path);

} // namespace vast::system
