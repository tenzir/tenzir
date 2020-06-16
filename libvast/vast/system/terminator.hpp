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

#include "vast/detail/type_traits.hpp"

#include <caf/actor_addr.hpp>
#include <caf/behavior.hpp>
#include <caf/response_promise.hpp>
#include <caf/stateful_actor.hpp>

#include <vector>

namespace vast::policy {

struct sequential {};
struct parallel {};

} // namespace vast::policy

namespace vast::system {

struct terminator_state {
  std::vector<caf::actor_addr> remaining_actors;
  caf::response_promise promise;
  static inline const char* name = "terminator";
};

template <class Policy>
caf::behavior terminator(caf::stateful_actor<terminator_state>* self);

/// @relates terminator
void shutdown(caf::event_based_actor* self, const caf::actor& terminator,
              std::vector<caf::actor> xs);

} // namespace vast::system
