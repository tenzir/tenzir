#ifndef VAST_DETAIL_SYSTEM_HPP
#define VAST_DETAIL_SYSTEM_HPP

#include <cstdint>
#include <string>

namespace vast {
namespace detail {

/// Retrieves the hostname of the system.
/// @returns The system hostname.
std::string hostname();

/// Retrieves the page size of the OS.
/// @returns The number of bytes of a page.
size_t page_size();

/// Retrieves the process ID.
/// @returns The ID of this process.
int32_t process_id();

} // namespace detail
} // namespace vast

#endif
