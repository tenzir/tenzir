#ifndef VAST_SYSTEM_TASK_HPP
#define VAST_SYSTEM_TASK_HPP

#include <map>

#include <caf/stateful_actor.hpp>

#include "vast/detail/flat_set.hpp"

#include "vast/system/atoms.hpp"

namespace vast {
namespace system {

struct task_state {
  uint64_t total = 0;
  caf::message done_msg;
  std::map<caf::actor_addr, uint64_t> workers;
  detail::flat_set<caf::actor> subscribers;
  detail::flat_set<caf::actor> supervisors;
  const char* name = "task";
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

} // namespace system
} // namespace vast

#endif
