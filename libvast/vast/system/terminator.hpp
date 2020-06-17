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

#include <vector>

namespace vast::policy {

struct sequential;
struct parallel;

} // namespace vast::policy

namespace vast::system {

struct terminator_state {
  std::vector<caf::actor_addr> remaining_actors;
  caf::response_promise promise;
  static inline const char* name = "terminator";
};

/// Performs a parallel shutdown of a list of actors.
template <class Policy>
caf::behavior terminator(caf::stateful_actor<terminator_state>* self);

} // namespace vast::system
