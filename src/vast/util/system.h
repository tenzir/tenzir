#ifndef VAST_UTIL_SYSTEM_H
#define VAST_UTIL_SYSTEM_H

#include <string>
#include "vast/util/trial.h"

namespace vast {
namespace util {

/// Retrieves the hostname of the system.
/// @returns The system hostname.
trial<std::string> hostname();

} // namespace util
} // namespace vast

#endif
