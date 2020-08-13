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

#include <caf/fwd.hpp>
#include <caf/response_promise.hpp>

#include <chrono>
#include <vector>

namespace vast::policy {

struct sequential;
struct parallel;

} // namespace vast::policy

namespace vast::system {

struct terminator_state {
  std::vector<caf::actor> remaining_actors;
  caf::response_promise promise;
  static inline const char* name = "terminator";
};

/// Performs a parallel shutdown of a list of actors.
/// @param self That terminator actor.
/// @param grace_period The timeout after which the terminator sends a
///        kill exit message to all remaining actors.
/// @param kill_timeout The timeout after which the terminator gives up
///        and exits, after having tried to kill remaining actors.
template <class Policy>
caf::behavior terminator(caf::stateful_actor<terminator_state>* self,
                         std::chrono::milliseconds grace_period,
                         std::chrono::milliseconds kill_timeout);

} // namespace vast::system
