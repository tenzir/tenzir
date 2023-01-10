//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/system/actors.hpp"
#include "vast/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <string>

namespace vast::system {

// TODO: Create a separate `passive_indexer_state`, similar to how partitions
// are handled.
struct indexer_state {
  constexpr static inline auto name = "index";

  /// The index holding the data.
  value_index_ptr idx;

  /// The partition id to which this indexer belongs (for log messages).
  uuid partition_id;

  /// The flat index of the column that the indexer is attached to.
  size_t column;

  /// Tracks whether we received at least one table slice column.
  bool stream_initiated;

  /// The response promise for a snapshot atom.
  caf::typed_response_promise<chunk_ptr> promise;
};

/// Indexes a table slice column with a single value index.
/// @param self A pointer to the spawned actor.
/// @param column The flat index of the column in the slice schema.
/// @param index The underlying value index.
/// @pre index
active_indexer_actor::behavior_type
active_indexer(active_indexer_actor::stateful_pointer<indexer_state> self,
               size_t column, value_index_ptr index);

/// An indexer that was recovered from on-disk state. It can only respond
/// to queries, but not add eny more entries.
/// @param self A pointer to the spawned actor.
/// @param uuid The id of the partition this indexer belongs to.
/// @param index The underlying value index.
/// @pre index
indexer_actor::behavior_type
passive_indexer(indexer_actor::stateful_pointer<indexer_state> self,
                uuid partition_id, value_index_ptr index);

} // namespace vast::system
