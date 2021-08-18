//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/expression.hpp"
#include "vast/legacy_type.hpp"
#include "vast/system/node.hpp"

#include <caf/actor.hpp>
#include <caf/fwd.hpp>

#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace vast::system {

struct pivoter_state {
  // -- member types -----------------------------------------------------------

  // -- constants --------------------------------------------------------------

  static inline constexpr const char* name = "pivoter";

  // -- constructors, destructors, and assignment operators --------------------

  pivoter_state(caf::event_based_actor* self);

  // -- member variables -------------------------------------------------------

  /// The name of the type that we are pivoting to.
  std::string target;

  /// The original query.
  /// TODO: Extract predicates that apply to the target type and extend the
  ///       generated queries with them. This depends on ECS support.
  expression expr;

  /// Keeps a record of the generic ids that were already queried, for the
  /// purpose of deduplication.
  /// TODO: We only need to query for membership, so this could be made more
  ///       efficient by storing the hash itself. Additionally, it is possible
  ///       for the field that represents the edge to be of another type than
  ///       string.
  std::unordered_set<std::string> requested_ids;

  /// A cache for the connections between a source type and the target type,
  /// to avoid multiple computations of those.
  mutable std::unordered_map<legacy_record_type, std::optional<record_field>>
    cache;

  /// A tracking counter of spawned exporters. Used for lifetime management.
  size_t running_exporters = 0;

  /// Flag that stores if the input source is done sending table slices. Used
  /// for lifetime management.
  bool initial_query_completed = false;

  /// Pointer to the parent actor.
  caf::stateful_actor<pivoter_state>* self;

  /// A handle to the parent node for spawning new EXPORTERs.
  node_actor node;

  /// A handle to the sink for the resulting table silces.
  caf::actor sink;
};

/// The PIVOTER receives table slices and constructs new queries for the target
/// type.
/// @param self The actor handle.
/// @param node The node actor to spawn exporters in.
/// @param target The type filter for the subsequent queries.
/// @param expression The query of the original command.
caf::behavior pivoter(caf::stateful_actor<pivoter_state>* self, node_actor node,
                      std::string target, expression expr);

} // namespace vast::system
