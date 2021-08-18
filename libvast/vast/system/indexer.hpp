//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/fbs/partition.hpp"
#include "vast/legacy_type.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <string>

namespace vast::system {

// TODO: Create a separate `passive_indexer_state`, similar to how partitions
// are handled.
struct indexer_state {
  /// The name of this indexer.
  std::string name;

  /// The index holding the data.
  value_index_ptr idx;

  /// Whether the type of this indexer has the `#skip` attribute, implying that
  /// the incoming data should not be indexed.
  bool has_skip_attribute;

  /// The partition id to which this indexer belongs (for log messages).
  uuid partition_id;

  /// Tracks whether we received at least one table slice column.
  bool stream_initiated;

  /// The response promise for a snapshot atom.
  caf::typed_response_promise<chunk_ptr> promise;
};

/// Indexes a table slice column with a single value index.
active_indexer_actor::behavior_type
active_indexer(active_indexer_actor::stateful_pointer<indexer_state> self,
               legacy_type index_type, caf::settings index_opts);

/// An indexer that was recovered from on-disk state. It can only respond
/// to queries, but not add eny more entries.
indexer_actor::behavior_type
passive_indexer(indexer_actor::stateful_pointer<indexer_state> self,
                uuid partition_id, value_index_ptr idx);

} // namespace vast::system
