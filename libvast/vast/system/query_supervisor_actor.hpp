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

#include "vast/system/index_client_actor.hpp"
#include "vast/system/partition_actor.hpp"
#include "vast/uuid.hpp"

#include <caf/detail/unordered_flat_map.hpp>
#include <caf/typed_actor.hpp>

#include <vector>

namespace vast::system {

/// A set of relevant partition actors, and their uuids.
using query_map = std::vector<std::pair<uuid, partition_actor>>;

/// The QUERY SUPERVISOR actor interface.
using query_supervisor_actor = caf::typed_actor<
  /// Reacts to an expression and a set of relevant partitions by
  /// sending several `vast::ids` to the index_client_actor, followed
  /// by a final `atom::done`.
  caf::reacts_to<expression, query_map, index_client_actor>>;

} // namespace vast::system
