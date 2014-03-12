#ifndef VAST_H
#define VAST_H

namespace vast {

/// Must be called before using any VAST construct.
/// @returns `true` on success.
bool initialize();

/// Cleans up global state.
/// @returns `true` on success.
bool cleanup();

} // namespace vast

#endif
