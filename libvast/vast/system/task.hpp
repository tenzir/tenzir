#ifndef VAST_ACTOR_TASK_HPP
#define VAST_ACTOR_TASK_HPP

#include <map>

#include "vast/actor/atoms.hpp"
#include "vast/actor/basic_state.hpp"
#include "vast/util/flat_set.hpp"

namespace vast {

/// An abstraction of a task where each work item consists of an actor. The
/// task completes as soon as all registered items send either a DONE atom or
/// terminate.
struct task {
  struct state : basic_state {
    state(local_actor* self);

    void complete(actor_addr const& addr);
    void notify();

    uint32_t exit_reason_ = exit::done;
    uint64_t total = 0;
    message done_msg;
    std::map<actor_addr, uint64_t> workers;
    util::flat_set<actor> subscribers;
    util::flat_set<actor> supervisors;
  };

  template <typename... Ts>
  static behavior make(stateful_actor<state>* self, Ts... xs) {
    return impl(self, make_message(done_atom::value, std::move(xs)...));
  }

private:
  static behavior impl(stateful_actor<state>* self, message done_msg);
};

} // namespace vast

#endif
