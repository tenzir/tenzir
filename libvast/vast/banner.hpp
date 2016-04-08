#ifndef VAST_BANNER_HPP
#define VAST_BANNER_HPP

#include <string>

namespace vast {

/// Returns the VAST banner in ASCII art.
/// @returns The VAST banner.
std::string banner(bool colorized = false);

} // namespace vast

#endif
