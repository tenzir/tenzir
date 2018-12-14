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

#include <cstdint>
#include <string>

#include <caf/detail/unordered_flat_map.hpp>
#include <caf/fwd.hpp>

#include "vast/ids.hpp"
#include "vast/uuid.hpp"

namespace vast::system {

/// Maps partition IDs to EVALUATOR actors.
using query_map = caf::detail::unordered_flat_map<uuid,
                                                  std::vector<caf::actor>>;

struct query_supervisor_state {
  // -- constructors, destructors, and assignment operators --------------------

  query_supervisor_state(caf::local_actor* self);

  // -- meber variables --------------------------------------------------------

  /// Maps partition IDs to the number of outstanding responses.
  caf::detail::unordered_flat_map<uuid, size_t> open_requests;

  // Gives the query_supervisor a unique, human-readable name in log output.
  std::string name;
};

caf::behavior
query_supervisor(caf::stateful_actor<query_supervisor_state>* self,
                 caf::actor master);

} // namespace vast::system
