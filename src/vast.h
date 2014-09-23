#ifndef VAST_H
#define VAST_H

#include "vast/configuration.h"
#include "vast/file_system.h"
#include "vast/logger.h"
#include "vast/program.h"
#include "vast/serialization.h"

namespace vast {

/// Cleans up global state.
/// @returns `true` on success.
bool cleanup();

} // namespace vast

#endif
