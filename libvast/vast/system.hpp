#ifndef VAST_SYSTEM_HPP
#define VAST_SYSTEM_HPP

#include <caf/actor_system_config.hpp>

namespace vast {

/// Creates an actor system config suitable for use in VAST.
/// @param cfg The actor system configuration.
/// @returns The actor system config.
caf::actor_system_config make_config();
caf::actor_system_config make_config(int argc, char** argv);

} // namespace vast

#endif
