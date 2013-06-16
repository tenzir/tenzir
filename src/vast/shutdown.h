#ifndef VAST_SHUTDOWN_H
#define VAST_SHUTDOWN_H

namespace vast {

/// Destroys all singletons and deletes global state.
void shutdown();

} // namespace vast

#endif
