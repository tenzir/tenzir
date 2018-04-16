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

#include <map>

#include <caf/behavior.hpp>
#include <caf/stateful_actor.hpp>

#include "vast/detail/flat_set.hpp"

#include "vast/system/atoms.hpp"

namespace vast::system {

struct task_state {
  uint64_t total = 0;
  caf::message done_msg;
  std::map<caf::actor_addr, uint64_t> workers;
  detail::flat_set<caf::actor> subscribers;
  detail::flat_set<caf::actor> supervisors;
  static inline const char* name = "task";
};

namespace detail {
caf::behavior task(caf::stateful_actor<task_state>* self, caf::message done);
} // namespace detail

/// An abstraction for work consisting of one or more actor. A work item
/// completes if the corresponding actor terminates or if one marks the actor
/// as complete with an explicit message. A task has *supervisors* and
/// *susbscribers*. Supervisors receive a special DONE message when the task
/// completes, with optional state passed to the task on construction.
/// Subscribers receive progress updates with each work item that completes.
template <class... Ts>
caf::behavior task(caf::stateful_actor<task_state>* self, Ts... xs) {
  auto done_msg = caf::make_message(done_atom::value, std::move(xs)...);
  return detail::task(self, std::move(done_msg));
}

} // namespace vast::system

