#ifndef VAST_BANNER_H
#define VAST_BANNER_H

#include <string>

namespace vast {

/// Returns the VAST banner in ASCII art.
/// @returns The VAST banner.
std::string banner(bool colorized = false);

} // namespace vast

#endif
