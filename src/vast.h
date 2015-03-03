#ifndef VAST_H
#define VAST_H

#include "vast/configuration.h"
#include "vast/filesystem.h"
#include "vast/logger.h"
#include "vast/serialization.h"
#include "vast/actor/program.h"
#include "vast/actor/signal_monitor.h"

namespace vast {

/// Cleans up global state.
/// @returns `true` on success.
bool cleanup();

} // namespace vast

#endif
