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

#include "vast/system/partition_client_actor.hpp"

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The INDEX CLIENT actor interface.
using index_client_actor = caf::typed_actor<
  // Receives done from the INDEX when the query finished.
  caf::reacts_to<atom::done>>
  // Receives ids from the INDEX for partial query hits.
  ::extend_with<partition_client_actor>;

} // namespace vast::system
