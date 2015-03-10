#ifndef VAST_DIE_H
#define VAST_DIE_H

#include <string>

namespace vast {

/// Terminates the process immediately.
/// @param msg The message to print before terminating.
[[noreturn]] void die(std::string const& = "");

} // namespace vast

#endif
