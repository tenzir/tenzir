#include "vast/actor/task.hpp"

namespace vast {

task::state::state(local_actor* self)
  : basic_state{self, "task"} {
}

void task::state::complete(actor_addr const& addr) {
  auto w = workers.find(addr);
  if (w == workers.end()) {
    VAST_ERROR_AT(self, "got completion signal from unknown actor:", addr);
    self->quit(exit::error);
  } else if (--w->second == 0) {
    self->demonitor(addr);
    workers.erase(w);
    notify();
  }
}

void task::state::notify() {
  for (auto& s : subscribers)
    self->send(s, progress_atom::value, uint64_t{workers.size()}, total);
  if (workers.empty()) {
    for (auto& s : supervisors)
      self->send(s, done_msg);
    self->quit(exit_reason_);
  }
}

behavior task::impl(stateful_actor<state>* self, message done_msg) {
  self->state.done_msg = std::move(done_msg);
  self->trap_exit(true);
  return {
    [=](exit_msg const& msg) {
      self->state.subscribers.clear();
      self->state.notify();
      self->quit(msg.reason);
    },
    [=](down_msg const& msg) {
      if (self->state.workers.erase(msg.source) == 1)
        self->state.notify();
    },
    [=](uint32_t exit_reason) {
      self->state.exit_reason_ = exit_reason;
    },
    [=](actor const& a) {
      VAST_TRACE_AT(self, "registers actor", a);
      self->monitor(a);
      ++self->state.workers[a.address()];
      ++self->state.total;
    },
    [=](actor const& a, uint64_t n) {
      VAST_TRACE_AT(self, "registers actor", a, "for", n, "sub-tasks");
      self->monitor(a);
      self->state.workers[a.address()] += n;
      ++self->state.total;
    },
    [=](done_atom, actor const& a) {
      VAST_TRACE_AT(self, "manually completed actor", a);
      self->state.complete(a->address());
    },
    [=](done_atom, actor_addr const& addr) {
      VAST_TRACE_AT(self, "manually completed actor with address", addr);
      self->state.complete(addr);
    },
    [=](done_atom) {
      VAST_TRACE_AT(self, "completed actor", self->current_sender());
      self->state.complete(self->current_sender());
    },
    [=](supervisor_atom, actor const& a) {
      VAST_TRACE_AT(self, "notifies", a, "about task completion");
      self->state.supervisors.insert(a);
    },
    [=](subscriber_atom, actor const& a) {
      VAST_TRACE_AT(self, "notifies", a, "on task status change");
      self->state.subscribers.insert(a);
    },
    [=](progress_atom) {
      auto num_workers = uint64_t{self->state.workers.size()};
      return make_message(num_workers, self->state.total);
    }
  };
}

} // namespace vast
