#ifndef VAST_ACTOR_SINK_BASE_H
#define VAST_ACTOR_SINK_BASE_H

#include "vast/actor/atoms.h"
#include "vast/actor/accountant.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/concept/printable/vast/event.h"
#include "vast/concept/printable/vast/time.h"
#include "vast/concept/printable/vast/uuid.h"
#include "vast/event.h"
#include "vast/time.h"
#include "vast/uuid.h"

namespace vast {
namespace sink {

// The base class for SINK actors.
struct state : basic_state {
  state(local_actor* self, char const* name)
    : basic_state{self, name} {
  }

  ~state() {
    flush();
  }

  virtual bool process(event const& e) = 0;
  virtual void flush() { /* nop */ }

  time::extent flush_interval = time::seconds(1); // TODO: make configurable
  time::moment last_flush;
  accountant::type accountant;
  uint64_t processed = 0;
  uint64_t limit = 0;
};

template <typename State>
behavior make(stateful_actor<State>* self) {
  self->state.last_flush = time::snapshot();
  auto handle = [=](event const& e) {
    if (!self->state.process(e)) {
      VAST_ERROR(self, "failed to process event:", e);
      self->quit(exit::error);
      return false;
    }
    if (++self->state.processed == self->state.limit) {
      VAST_VERBOSE_AT(self, "reached limit: ", self->state.limit, "events");
      self->quit(exit::done);
    }
    auto now = time::snapshot();
    if (now - self->state.last_flush > self->state.flush_interval) {
      self->state.flush();
      self->state.last_flush = now;
    }
    return true;
  };
  return {
    [=](limit_atom, uint64_t max) {
      VAST_DEBUG_AT(self, "caps event export at", max, "events");
      if (self->state.processed < max)
        self->state.limit = max;
      else
        VAST_WARN_AT(self, "ignores new limit of", max, "(already processed",
                     self->state.processed, " events)");
    },
    [=](accountant::type acc) {
      VAST_DEBUG_AT(self, "registers accountant#" << acc->id());
      self->state.accountant = acc;
    },
    [=](uuid const&, event const& e) { handle(e); },
    [=](uuid const&, std::vector<event> const& v) {
      assert(!v.empty());
      for (auto& e : v)
        if (!handle(e))
          return;
    },
    [=](uuid const& id, progress_atom, double progress, uint64_t total_hits) {
      VAST_VERBOSE_AT(self, "got progress from query ", id << ':', total_hits,
                      "hits (" << size_t(progress * 100) << "%)");
    },
    [=](uuid const& id, done_atom, time::extent runtime) {
      VAST_VERBOSE_AT(self, "got DONE from query", id << ", took", runtime);
    },
    log_others(self)
  };
}

} // namespace sink
} // namespace vast

#endif
