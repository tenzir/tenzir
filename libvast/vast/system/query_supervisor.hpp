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

#include "vast/fwd.hpp"
#include "vast/ids.hpp"
#include "vast/system/evaluator.hpp"
#include "vast/system/query_supervisor_actor.hpp"
#include "vast/system/query_supervisor_master_actor.hpp"
#include "vast/uuid.hpp"

#include <caf/detail/unordered_flat_map.hpp>

#include <cstdint>
#include <string>

namespace vast::system {

/// The internal state of a QUERY SUPERVISOR actor.
struct query_supervisor_state {
  explicit query_supervisor_state(
    query_supervisor_actor::stateful_pointer<query_supervisor_state> self);

  /// Maps partition IDs to the number of outstanding responses.
  caf::detail::unordered_flat_map<uuid, size_t> open_requests;

  // Gives the query_supervisor a unique, human-readable name in log output.
  std::string name;
};

/// Returns the behavior of a QUERY SUPERVISOR actor.
/// @param self The stateful self pointer to the QUERY SUPERVISOR.
/// @param master The actor this QUERY SUPERVISOR reports to.
query_supervisor_actor::behavior_type query_supervisor(
  query_supervisor_actor::stateful_pointer<query_supervisor_state> self,
  query_supervisor_master_actor master);

} // namespace vast::system
