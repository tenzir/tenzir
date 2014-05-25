#ifndef VAST_H
#define VAST_H

#include "vast/configuration.h"
#include "vast/program.h"

namespace vast {

/// Cleans up global state.
/// @returns `true` on success.
bool cleanup();

} // namespace vast

#endif
