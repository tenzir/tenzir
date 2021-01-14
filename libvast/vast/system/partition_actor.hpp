/******************************************************************************

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

#include "vast/system/partition_client_actor.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The PARTITION actor interface.
using partition_actor = caf::typed_actor<
  // Evaluate the given expression, returning the relevant evaluation triples.
  // TODO: Passing the `partition_client_actor` here is an historical artifact,
  //       a cleaner API would be to just return the evaluated `vast::ids`.
  caf::replies_to<expression, partition_client_actor>::with<atom::done>>;

} // namespace vast::system
