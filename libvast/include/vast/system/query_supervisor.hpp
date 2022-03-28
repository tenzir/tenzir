//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/ids.hpp"
#include "vast/system/actors.hpp"
#include "vast/uuid.hpp"

#include <caf/detail/unordered_flat_map.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scheduled_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <cstdint>
#include <string>

namespace vast::system {

/// The internal state of a QUERY SUPERVISOR actor.
struct query_supervisor_state {
  explicit query_supervisor_state(
    query_supervisor_actor::stateful_pointer<query_supervisor_state> self);

  /// The set of queries currently in progress. This should
  /// have size <= 1 in normal operation.
  std::set<vast::uuid> in_progress;

  /// The master of the QUERY SUPERVISOR.
  query_supervisor_master_actor master;

  static inline const char* name = "query-supervisor";
};

/// Returns the behavior of a QUERY SUPERVISOR actor.
/// @param self The stateful self pointer to the QUERY SUPERVISOR.
/// @param master The actor this QUERY SUPERVISOR reports to.
query_supervisor_actor::behavior_type query_supervisor(
  query_supervisor_actor::stateful_pointer<query_supervisor_state> self,
  query_supervisor_master_actor master);

} // namespace vast::system
