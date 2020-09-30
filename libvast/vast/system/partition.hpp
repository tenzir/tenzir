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
#include "vast/system/filesystem.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"
#include "vast/value_index.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/stream_slot.hpp>

#include <unordered_map>

#include "caf/fwd.hpp"

namespace vast::system {

/// Helper class used to route table slice columns to the correct indexer
/// in the CAF stream stage.
struct partition_selector {
  bool operator()(const vast::qualified_record_field& filter,
                  const table_slice_column& x) const;
};

/// The state of the active partition actor.
struct active_partition_state {
  using partition_stream_stage_ptr = caf::stream_stage_ptr<
    table_slice_ptr,
    caf::broadcast_downstream_manager<
      table_slice_column, vast::qualified_record_field, partition_selector>>;

  caf::actor indexer_at(size_t position) const;

  /// Data Members

  /// Pointer to the parent actor.
  caf::stateful_actor<active_partition_state>* self;

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
  detail::stable_map<qualified_record_field, caf::actor> indexers;

  /// Maps type names to IDs. Used the answer #type queries.
  std::unordered_map<std::string, ids> type_ids;

  /// A local version of the meta index that only contains the entry
  /// related for this partition. During startup, the INDEX reads all
  /// of these from the flatbufferized state to build up the global
  /// meta index.
  meta_index meta_idx;

  /// A readable name for this partition
  std::string name;

  /// The first ID in the partition.
  vast::id offset;

  /// The number of events in the partition.
  size_t events;

  /// Actor handle of the filesystem actor.
  filesystem_type fs_actor;

  /// Promise that gets satisfied when the partition state was serialized
  /// and written to disk.
  caf::response_promise persistence_promise;

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

  caf::actor indexer_at(size_t position) const;

  /// Pointer to the parent actor.
  caf::stateful_actor<passive_partition_state>* self;

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

  /// A typed view into the `partition_chunk`.
  const fbs::partition::v0* flatbuffer;

  /// Maps qualified fields to indexer actors. This is mutable since
  /// indexers are spawned lazily on first access.
  mutable std::vector<caf::actor> indexers;
};

// Flatbuffer support

caf::expected<flatbuffers::Offset<fbs::partition::v0>>
pack(flatbuffers::FlatBufferBuilder& builder, const active_partition_state& x);

caf::error unpack(const fbs::partition::v0& x, passive_partition_state& y);

caf::error unpack(const fbs::partition::v0& x, partition_synopsis& y);

// TODO: Use typed actors for the partition actors.

/// Spawns a partition.
/// @param self The partition actor.
/// @param id The UUID of this partition.
/// @param fs The actor handle of the filesystem actor.
/// @param index_opts Settings that are forwarded when creating indexers.
caf::behavior
active_partition(caf::stateful_actor<active_partition_state>* self, uuid id,
                 filesystem_type fs, caf::settings index_opts, meta_index);

/// Spawns a read-only partition.
/// @param self The partition actor.
/// @param id The UUID of this partition.
/// @param fs The actor handle of the filesystem actor.
/// @param path The path where the partition flatbuffer can be found.
caf::behavior
passive_partition(caf::stateful_actor<passive_partition_state>* self, uuid id,
                  filesystem_type fs, vast::path path);

} // namespace vast::system
