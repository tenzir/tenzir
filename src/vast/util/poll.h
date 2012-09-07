#ifndef VAST_UTIL_POLL_H
#define VAST_UTIL_POLL_H

namespace vast {
namespace util {

/// Polls a file descriptor for ready read events.
/// @param fd The file descriptor to poll
/// @param usec The number of microseconds to wait.
/// @return `true` if *fd* has ready events for reading.
bool poll(int fd, int usec = 100000);

} // namespace util
} // namespace vast

#endif
