#ifndef VAST_ACTOR_SOURCE_SPAWN_H
#define VAST_ACTOR_SOURCE_SPAWN_H

#include <caf/actor.hpp>
#include <caf/message.hpp>

#include "vast/trial.h"

namespace vast {
namespace source {

trial<caf::actor> spawn(caf::message const& params);

} // namespace source
} // namespace vast

#endif
