#ifndef VAST_UTIL_CONSOLE_H
#define VAST_UTIL_CONSOLE_H

namespace vast {
namespace util {
namespace console {

/// Makes the console unbuffered.
void unbuffer();

/// Makes the console buffered again after a call to ::unbuffer.
/// @pre `unbuffer()` must have been called beforehand.
void buffer();

/// Tries to extract a single a character from STDIN within a short time
/// interval. The function blocks until either input arrives or the timeout
/// expires.
///
/// @param c The character from STDIN.
///
/// @param timeout The maximum number of milliseconds to block.
///
/// @return `true` *iff* extracting a character from STDIN was successful
/// within *timeout* milliseconds.
///
/// @post Iff the function returned `true`, *c* contains the next character
/// from STDIN.
bool get(char& c, int timeout = 100);

} // namespace console
} // namespace util
} // namespace vast

#endif
