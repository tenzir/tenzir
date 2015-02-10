#ifndef VAST_UTIL_SYSTEM_H
#define VAST_UTIL_SYSTEM_H

#include <string>
#include "vast/util/trial.h"

namespace vast {
namespace util {

/// Retrieves the hostname of the system.
/// @returns The system hostname.
trial<std::string> hostname();

/// Retrieves the process ID.
/// @returns The ID of this process.
int32_t process_id();

} // namespace util
} // namespace vast

#endif
