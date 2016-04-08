#ifndef VAST_UTIL_SYSTEM_HPP
#define VAST_UTIL_SYSTEM_HPP

#include <cstdint>
#include <string>

namespace vast {
namespace util {

/// Retrieves the hostname of the system.
/// @returns The system hostname.
std::string hostname();

/// Retrieves the process ID.
/// @returns The ID of this process.
int32_t process_id();

} // namespace util
} // namespace vast

#endif
