/******************************************************************************
 *                    _   _____   __________                                  *
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

#include "vast/fbs/partition.hpp"
#include "vast/fwd.hpp"
#include "vast/path.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/filesystem.hpp"
#include "vast/system/instrumentation.hpp"
#include "vast/type.hpp"
#include "vast/uuid.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/stateful_actor.hpp>

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
  caf::response_promise promise;
};

/// Indexes a table slice column with a single value index.
caf::behavior active_indexer(caf::stateful_actor<indexer_state>* self, type index_type,
                      caf::settings index_opts);

/// An indexer that was recovered from on-disk state. It can only respond
/// to queries, but not add eny more entries.
caf::behavior passive_indexer(caf::stateful_actor<indexer_state>* self,
                               uuid partition_id, value_index_ptr idx);

} // namespace vast::system
