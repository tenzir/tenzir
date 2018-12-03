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

/// Maps partition IDs to EVALUATOR actors (1 per layout in the partition).
using query_map = caf::detail::unordered_flat_map<uuid,
                                                  std::vector<caf::actor>>;

struct collector_state {
  // -- constructors, destructors, and assignment operators --------------------

  collector_state(caf::local_actor* self);

  // -- meber variables --------------------------------------------------------

  /// Maps partition IDs to the number of outstanding responses and already
  /// received event IDs.
  caf::detail::unordered_flat_map<uuid, std::pair<size_t, ids>> open_requests;

  // Gives the COLLECTOR a unique, human-readable name in log output.
  std::string name;
};

caf::behavior collector(caf::stateful_actor<collector_state>* self,
                        caf::actor master);

} // namespace vast::system
