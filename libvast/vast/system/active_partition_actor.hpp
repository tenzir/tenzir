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

#include "vast/system/index_actor.hpp"
#include "vast/system/partition_actor.hpp"

#include <caf/typed_actor.hpp>

namespace vast::system {

/// The interface of an ACTIVE PARTITION actor.
using active_partition_actor = caf::typed_actor<
  // Hooks into the table slice stream.
  caf::replies_to<caf::stream<table_slice>>::with< //
    caf::inbound_stream_slot<table_slice>>,
  // Persists the active partition at the specified path.
  caf::replies_to<atom::persist, path, index_actor>::with< //
    atom::ok>,
  // A repeatedly called continuation of the persist request.
  caf::reacts_to<atom::persist, atom::resume>>
  // Conform to the protocol of the PARTITION.
  ::extend_with<partition_actor>;

} // namespace vast::system
