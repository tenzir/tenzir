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

#include "vast/column_index.hpp"
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

namespace v2 {

// TODO: Create a separate `passive_indexer_state`, similar to how partitions
// are handled.
struct indexer_state {
  value_index_ptr idx;

  /// The name of this indexer.
  std::string name;

  /// The partition id to which this indexer belongs (for debugging).
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

} // namespace v2

struct indexer_state {
  // -- constructors, destructors, and assignment operators --------------------

  indexer_state();

  ~indexer_state();

  // -- member functions -------------------------------------------------------

  caf::error init(caf::event_based_actor* self, path filename, type column_type,
                  caf::settings index_opts, caf::actor index, uuid partition_id,
                  std::string fqn);

  void send_report();

  // -- member variables -------------------------------------------------------

  union { column_index col; };

  caf::actor index;

  accountant_type accountant;

  caf::event_based_actor* self;

  uuid partition_id;

  std::string fqn;

  measurement m;

  bool streaming_done;

  static inline const char* name = "indexer";
};

/// Indexes a single column of table slices.
/// @param self The actor handle.
/// @param filename The file in which to store the index column.
/// @param column_type The type of the indexed column.
/// @param index_opts Runtime options to parameterize the value index.
/// @param index A handle to the index actor.
/// @param partition_id The partition ID that this INDEXER belongs to.
/// @param fqn The fully-qualified name of the indexed column.
/// @returns the initial behavior of the INDEXER.
caf::behavior indexer(caf::stateful_actor<indexer_state>* self, path filename,
                      type column_type, caf::settings index_opts,
                      caf::actor index, uuid partition_id, std::string fqn);

} // namespace vast::system
