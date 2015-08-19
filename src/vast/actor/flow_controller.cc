#include <caf/all.hpp>

#include "vast/actor/flow_controller.h"

namespace vast {
namespace flow_controller {

namespace {

template <typename Signal>
void propagate(stateful_actor<state>* self, Signal signal,
               caf::actor const& vertex, caf::actor const& start) {
  auto const& graph = self->state.graph;
  auto er = graph.equal_range(start);
  // Terminate recursion: no further upstream nodes.
  if (er.first == er.second) {
    if (start == vertex) {
      VAST_WARN_AT(self, "got unhandled signal:", start, "has no source");
      return;
    }
    // By definition, a deflector must not be the source of data flow and
    // cannot occur here. Thus, we must have reached a source at this point.
    VAST_DEBUG_AT(self, "propagates signal from", vertex, "to source", start);
    self->send(message_priority::high, start, signal, vertex);
    return;
  }
  while (er.first != er.second) {
    auto source = &er.first->second;
    if (self->state.deflectors.count(*source) > 0) {
      // If any of the vertices act as deflector, we can ignore the the current
      // branch and deflect the signal.
      VAST_DEBUG_AT(self, "deflects signal to", *source);
      self->send(message_priority::high, *source, signal, vertex);
      return;
    } else {
      propagate(self, signal, vertex, *source);
    }
    ++er.first;
  }
}

} // namespace <anonymous>

state::state(event_based_actor* self)
  : basic_state{self, "flow-controller"} {
}

behavior actor(stateful_actor<state>* self) {
  using caf::actor;
  return {
    [=](down_msg const& msg) {
      self->state.deflectors.erase(actor_cast<actor>(msg.source));
      auto i = self->state.graph.begin();
      while (i != self->state.graph.end())
        if (i->first->address() == msg.source
            || i->second.address() == msg.source)
          i = self->state.graph.erase(i);
        else
          ++i;
    },
    [=](add_atom, actor const& source, actor const& sink) {
      VAST_DEBUG_AT(self, "inserts reverse edge:", sink, "->", source);
      auto i = self->state.graph.find(sink);
      if (i != self->state.graph.end() && i->second == source) {
        VAST_WARN_AT(self, "got duplicate edge registration");
        return;
      }
      self->state.graph.emplace(sink, source);
      self->monitor(source);
      self->monitor(sink);
    },
    [=](add_atom, deflector_atom, actor const& deflector) {
      VAST_DEBUG_AT(self, "injects deflector", deflector);
      if (self->state.deflectors.count(deflector) > 0) {
        VAST_WARN_AT(self, "got duplicate deflector injection");
        return;
      }
      auto& graph = self->state.graph;
      // A deflector must exist along an existing path and cannot be a source.
      auto d = graph.find(deflector);
      if (d == graph.end()) {
        VAST_ERROR_AT(self, "could not find intermediate vertex");
        return;
      }
      // Neither can it be a data sink. This means that there exists at least
      // one edge (X,Y) such that Y is the deflector.
      auto is_sink = true;
      for (auto i = graph.begin(); i != graph.end(); ++i)
        if (i->second == d->first) {
          is_sink = false;
          break;
        }
      if (is_sink) {
        VAST_ERROR_AT(self, "deflector cannot be a sink");
        return;
      }
      self->state.deflectors.insert(deflector);
    },
    [=](overload_atom) {
      auto sender = actor_cast<actor>(self->current_sender());
      VAST_DEBUG_AT(self, "got OVERLOAD from", sender);
      propagate(self, overload_atom::value, sender, sender);
    },
    [=](underload_atom) {
      auto sender = actor_cast<actor>(self->current_sender());
      VAST_DEBUG_AT(self, "got UNDERLOAD from", sender);
      propagate(self, underload_atom::value, sender, sender);
    },
    [=](overload_atom, actor const& vertex) {
      auto sender = actor_cast<actor>(self->current_sender());
      VAST_DEBUG_AT(self, "got OVERLOAD from", sender);
      propagate(self, overload_atom::value, vertex, sender);
    },
    [=](underload_atom, actor const& vertex) {
      auto sender = actor_cast<actor>(self->current_sender());
      VAST_DEBUG_AT(self, "got UNDERLOAD from", sender);
      propagate(self, underload_atom::value, vertex, sender);
    }
  };
}

} // namespace flow_controller
} // namespace vast
