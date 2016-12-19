#include "vast/logger.hpp"

#include <caf/all.hpp>

#include "vast/error.hpp"

#include "vast/system/task.hpp"

using namespace caf;

namespace vast {
namespace system {
namespace {

template <class Actor>
void notify(Actor self) {
  for (auto& s : self->state.subscribers)
    self->send(s, progress_atom::value, uint64_t{self->state.workers.size()},
               self->state.total);
  if (self->state.workers.empty()) {
    for (auto& s : self->state.supervisors)
      self->send(s, self->state.done_msg);
    self->quit();
  }
};

template <class Actor>
void complete(Actor self, actor_addr const& a) {
  auto w = self->state.workers.find(a);
  if (w == self->state.workers.end()) {
    VAST_ERROR(self, "got completion signal from unknown actor:", a);
    self->quit(make_error(ec::unspecified, "got DONE from unknown actor"));
  } else if (--w->second == 0) {
    self->demonitor(a);
    self->state.workers.erase(w);
    notify(self);
  }
};

} // namespace <anonymous>

namespace detail {

behavior task(stateful_actor<task_state>* self, message done_msg) {
  self->state.done_msg = std::move(done_msg);
  self->set_exit_handler(
    [=](exit_msg const& msg) {
      self->state.subscribers.clear();
      notify(self);
      self->quit(msg.reason);
    }
  );
  self->set_down_handler(
    [=](down_msg const& msg) {
      if (self->state.workers.erase(msg.source) == 1)
        notify(self);
    }
  );
  return {
    [=](actor const& a) {
      VAST_TRACE(self, "registers actor", a);
      self->monitor(a);
      if (++self->state.workers[a.address()] == 1)
        ++self->state.total;
    },
    [=](actor const& a, uint64_t n) {
      VAST_TRACE(self, "registers actor", a, "for", n, "sub-tasks");
      self->monitor(a);
      self->state.workers[a.address()] += n;
      ++self->state.total;
    },
    [=](done_atom, actor_addr const& addr) {
      VAST_TRACE(self, "manually completed actor with address", addr);
      complete(self, addr);
    },
    [=](done_atom) {
      VAST_TRACE(self, "completed actor", self->current_sender());
      complete(self, actor_cast<actor_addr>(self->current_sender()));
    },
    [=](supervisor_atom, actor const& a) {
      VAST_TRACE(self, "notifies actor", a, "about task completion");
      self->state.supervisors.insert(a);
    },
    [=](subscriber_atom, actor const& a) {
      VAST_TRACE(self, "notifies actor", a, "on task status change");
      self->state.subscribers.insert(a);
    },
    [=](progress_atom) {
      auto num_workers = uint64_t{self->state.workers.size()};
      return make_message(num_workers, self->state.total);
    }
  };
}

} // namespace detail

} // namespace system
} // namespace vast
