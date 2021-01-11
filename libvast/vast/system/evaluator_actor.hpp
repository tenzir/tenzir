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

#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

/// The EVALUATOR actor interface.
using evaluator_actor = caf::typed_actor<
  // Re-evaluates the expression and relays new hits to the INDEX CLIENT.
  caf::replies_to<partition_client_actor>::with<atom::done>>;

} // namespace vast::system
