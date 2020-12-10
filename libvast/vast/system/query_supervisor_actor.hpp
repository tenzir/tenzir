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

#include "vast/system/evaluator_actor.hpp"
#include "vast/uuid.hpp"

#include <caf/detail/unordered_flat_map.hpp>
#include <caf/typed_actor.hpp>

#include <vector>

namespace vast::system {

/// Maps partition IDs to EVALUATOR actors.
using query_map
  = caf::detail::unordered_flat_map<uuid, std::vector<evaluator_actor>>;

/// The QUERY SUPERVISOR actor interface.
using query_supervisor_actor = caf::typed_actor<
  // Supervise the evaluation of a query by the EVALUATOR actors, and forward
  // the results to the INDEX CLIENT.
  caf::reacts_to<expression, query_map, index_client_actor>>;

} // namespace vast::system
