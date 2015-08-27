#ifndef VAST_ACTOR_BASIC_STATE_H
#define VAST_ACTOR_BASIC_STATE_H

#include <string>
#include <ostream>

#include <caf/stateful_actor.hpp>

#include "vast/util/assert.h"

namespace caf {

template <typename Char, typename Traits, typename T>
std::basic_ostream<Char, Traits>&
operator<<(std::basic_ostream<Char, Traits>& out, stateful_actor<T> const* a) {
  VAST_ASSERT(a != nullptr);
  out << a->name() << '#' << a->id();
  return out;
}

} // namespace caf

#include "vast/logger.h"
#include "vast/actor/exit.h"

namespace vast {

// TODO: include all key CAF elements, we treat them like family.
using caf::actor;
using caf::behavior;
using caf::event_based_actor;
using caf::local_actor;
using caf::others;
using caf::stateful_actor;

/// The base class for actor state.
struct basic_state  {
  caf::local_actor* self;
  std::string name;

  basic_state(caf::local_actor* self, std::string name)
    : self{self}, name{std::move(name)} {
    VAST_DEBUG_AT(this->name << '#' << self->id(), "spawned");
  }

  ~basic_state() {
    auto rc = render_exit_reason(self->planned_exit_reason());
    VAST_DEBUG_AT(name << '#' << self->id(), "terminated (" << rc << ')');
  };
};

/// Helper to inject a catch-all match expression that logs unexpected
/// messages.
/// @param self The actor context.
auto log_others = [](auto self) {
  using namespace caf;
  return others >> [=] {
    VAST_ERROR_AT(self, "got unexpected message from",
                  '#' << self->current_sender()->id() << ':',
                  to_string(self->current_message()));
  };
};

auto quit_on_others = [](auto self) {
  using namespace caf;
  return others >> [=] {
    VAST_ERROR_AT(self, "got unexpected message from",
                  '#' << self->current_sender()->id() << ':',
                  to_string(self->current_message()));
    self->quit(exit::error);
  };
};

} // namespace vast

#endif
