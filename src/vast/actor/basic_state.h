#ifndef VAST_ACTOR_BASIC_STATE_H
#define VAST_ACTOR_BASIC_STATE_H

#include <string>

#include "vast/logger.h"
#include "vast/actor/caf.h"

namespace vast {

/// The base class for actor state.
struct basic_state  {
  local_actor* self;
  std::string name;

  basic_state(local_actor* self, std::string name)
    : self{self}, name{std::move(name)} {
    VAST_DEBUG_AT(this->name << '#' << self->id(), "spawned");
  }

  ~basic_state() {
    auto rc = render_exit_reason(self->planned_exit_reason());
    VAST_DEBUG_AT(name << '#' << self->id(), "terminated (" << rc << ')');
  };
};

} // namespace vast

#endif
