#ifndef VAST_ACTOR_SOURCE_SPAWN_HPP
#define VAST_ACTOR_SOURCE_SPAWN_HPP

#include <caf/actor.hpp>
#include <caf/message.hpp>

#include "vast/trial.hpp"

namespace vast {
namespace source {

trial<caf::actor> spawn(caf::message const& params);

} // namespace source
} // namespace vast

#endif
