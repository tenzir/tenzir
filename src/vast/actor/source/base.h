#ifndef VAST_ACTOR_SOURCE_BASE_H
#define VAST_ACTOR_SOURCE_BASE_H

#include "vast/event.h"
#include "vast/result.h"
#include "vast/actor/atoms.h"
#include "vast/actor/basic_state.h"
#include "vast/concept/printable/vast/error.h"
#include "vast/util/assert.h"

namespace vast {
namespace source {

/// The base class for derived states which extract events.
struct base_state : basic_state {
  base_state(local_actor* self, char const* name)
    : basic_state{self, name} {
  }

  ~base_state() {
    if (!events.empty())
      self->send(sinks[next_sink++ % sinks.size()], std::move(events));
  }

  virtual vast::schema schema() = 0;
  virtual void schema(vast::schema const& sch) = 0;
  virtual result<event> extract() = 0;

  bool done = false;
  bool paused = false;
  actor accountant;
  std::vector<actor> sinks;
  size_t next_sink = 0;
  uint64_t batch_size = std::numeric_limits<uint16_t>::max();
  std::vector<event> events;
};

/// The base actor for sources.
/// @param self The actor handle.
template <typename State>
behavior base(stateful_actor<State>* self) {
  using caf::actor;
  return {
    [=](down_msg const& msg) {
      // Handle sink termination.
      auto sink = std::find_if(
        self->state.sinks.begin(),
        self->state.sinks.end(),
        [&](auto& x) { return msg.source == x; }
      );
      if (sink != self->state.sinks.end())
        self->state.sinks.erase(sink);
      if (self->state.sinks.empty()) {
        VAST_WARN_AT(self, "has no more sinks");
        self->quit(exit::done);
      }
    },
    [=](overload_atom, actor const& victim) {
      VAST_DEBUG_AT(self, "got OVERLOAD signal from", victim);
      self->state.paused = true; // Stop after the next batch.
    },
    [=](underload_atom, actor const& victim) {
      VAST_DEBUG_AT(self, "got UNDERLOAD signal from", victim);
      self->state.paused = false;
      if (!self->state.done)
        self->send(self, run_atom::value);
    },
    [=](batch_atom, uint64_t batch_size) {
      VAST_DEBUG_AT(self, "sets batch size to", batch_size);
      self->state.batch_size = batch_size;
    },
    [=](get_atom, schema_atom) {
      return self->state.schema();
    },
    [=](put_atom, schema const& sch) {
      self->state.schema(sch);
    },
    [=](put_atom, sink_atom, actor const& sink) {
      VAST_DEBUG_AT(self, "adds sink to", sink);
      self->monitor(sink);
      self->send(sink, upstream_atom::value, self);
      self->state.sinks.push_back(sink);
    },
    [=](put_atom, accountant_atom, actor const& accountant) {
      VAST_DEBUG_AT(self, "registers accountant", accountant);
      self->state.accountant = accountant;
      auto label = self->name() + '#' + to_string(self->id()) + "-events";
      self->send(self->state.accountant, std::move(label), time::now());
    },
    [=](get_atom, sink_atom) { return self->state.sinks; },
    [=](run_atom) {
      if (self->state.sinks.empty()) {
        VAST_ERROR_AT(self, "cannot run without sinks");
        self->quit(exit::error);
        return;
      }
      while (self->state.events.size() < self->state.batch_size
             && !self->state.done) {
        auto r = self->state.extract();
        if (r) {
          self->state.events.push_back(std::move(*r));
        } else if (r.failed()) {
          VAST_ERROR_AT(self, r.error());
          self->state.done = true;
          break;
        }
      }
      if (!self->state.events.empty()) {
        VAST_VERBOSE_AT(self, "produced", self->state.events.size(), "events");
        if (self->state.accountant != invalid_actor)
          self->send(self->state.accountant,
                     uint64_t{self->state.events.size()}, time::snapshot());
        auto next = self->state.next_sink++ % self->state.sinks.size();
        self->send(self->state.sinks[next], std::move(self->state.events));
        self->state.events = {};
        // FIXME: if we do not give the stdlib implementation a hint to yield
        // here, this actor can monopolize all available resources. In
        // particular, we encountered a scenario where it prevented the BASP
        // broker from a getting a chance to operate, thereby queuing up
        // all event batches locally and running out of memory, as opposed to
        // sending them out as soon as possible. This yield fix temporarily
        // works around a deeper issue in CAF, which needs to be addressed in
        // the future.
        std::this_thread::yield();
      }
      if (self->state.done)
        self->quit(exit::done);
      else if (!self->state.paused)
        self->send(self, self->current_message());
    },
    quit_on_others(self),
  };
}

} // namespace source
} // namespace vast

#endif
