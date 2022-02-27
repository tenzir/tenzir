//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/expression.hpp"
#include "vast/system/node.hpp"

#include <caf/actor.hpp>
#include <caf/fwd.hpp>

#include <string>
#include <unordered_map>
#include <unordered_set>

namespace vast::system {

struct explorer_state {
  struct event_limits {
    uint64_t total;
    uint64_t per_result;
    uint64_t initial_query;
  };

  static inline constexpr const char* name = "explorer";

  explorer_state(caf::event_based_actor* self);

  /// Send the results to the sink, after removing duplicates.
  void forward_results(vast::table_slice slice);

  /// Maximum number of events to output.
  event_limits limits;

  /// Number of events sent to the source.
  size_t num_sent;

  /// Size of the timespan prior to each event.
  std::optional<vast::duration> before;

  /// Size of the timespan after each event.
  std::optional<vast::duration> after;

  /// Field by which to restrict the result set for each event.
  std::optional<std::string> by;

  /// Keeps a record of the ids that were already returned to the sink,
  /// for the purpose of deduplication.
  std::unordered_set<size_t> returned_ids;

  /// A tracking counter of spawned exporters. Used for lifetime management.
  size_t running_exporters = 0;

  /// Flag that stores if the input source is done sending table slices. Used
  /// for lifetime management.
  bool initial_query_completed = false;

  uint64_t initial_query_results = 0;

  /// A handle to the exporter sending us the results of the original query.
  caf::actor_addr initial_exporter = {};

  /// Pointer to the parent actor.
  caf::stateful_actor<explorer_state>* self;

  /// A handle to the parent node for spawning new EXPORTERs.
  node_actor node;

  /// A handle to the sink for the resulting table silces.
  caf::actor sink;
};

/// The EXPLORER receives table slices and constructs new queries for a time box
/// around each result.
/// @param self The actor handle.
/// @param node The node actor to spawn exporters in.
/// @param before Size of the time box prior to each result.
/// @param after Size of the time box after each result.
/// @param by Field by which to restrict the result set for each element.
caf::behavior
explorer(caf::stateful_actor<explorer_state>* self, node_actor node,
         explorer_state::event_limits limits,
         std::optional<vast::duration> before,
         std::optional<vast::duration> after, std::optional<std::string> by);

} // namespace vast::system
