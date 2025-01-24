//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/retention_policy.hpp"
#include "tenzir/table_slice.hpp"

#include <caf/typed_event_based_actor.hpp>
#include <caf/typed_response_promise.hpp>

#include <filesystem>
#include <vector>

namespace tenzir {

/// Receives chunks from SOURCEs, imbues them with an ID, and relays them to
/// INDEX and continuous queries.
struct importer_state {
  // -- member types -----------------------------------------------------------

  explicit importer_state(importer_actor::pointer self);

  ~importer_state();

  void send_report();

  void on_process(const table_slice& slice);

  /// Process a slice and forward it to the index.
  void handle_slice(table_slice&& slice);

  /// Pointer to the owning actor.
  importer_actor::pointer self;

  detail::flat_map<type, uint64_t> schema_counters = {};

  /// The index actor and the policy for retention.
  index_actor index;
  retention_policy retention_policy = {};

  /// Potentially unpersisted events.
  std::vector<table_slice> unpersisted_events = {};

  /// A list of subscribers for incoming events.
  std::vector<std::pair<receiver_actor<table_slice>, bool /*internal*/>>
    subscribers = {};

  /// Name of this actor in log events.
  static inline const char* name = "importer";
};

/// Spawns an IMPORTER.
/// @param self The actor handle.
/// @param dir The directory for persistent state.
/// @param index A handle to the INDEX.
/// @param retention_policy The retention policy to apply.
importer_actor::behavior_type
importer(importer_actor::stateful_pointer<importer_state> self,
         const std::filesystem::path& dir, index_actor index);

} // namespace tenzir
