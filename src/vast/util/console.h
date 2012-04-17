#ifndef VAST_UTIL_CONSOLE_H
#define VAST_UTIL_CONSOLE_H

namespace vast {
namespace util {

/// Makes the console unbuffered.
void unbuffer();

/// Makes the console buffered again after a call to unbuffer.
void buffer();

} // namespace util
} // namespace vast

#endif
