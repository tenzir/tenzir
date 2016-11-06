#ifndef VAST_ACTOR_SINK_SPAWN_HPP
#define VAST_ACTOR_SINK_SPAWN_HPP

#include "vast/caf.hpp"
#include "vast/trial.hpp"

namespace vast {
namespace sink {

trial<actor> spawn(message const& params);

} // namespace sink
} // namespace vast

#endif
