#ifndef VAST_UTIL_DETAIL_POSIX_HPP
#define VAST_UTIL_DETAIL_POSIX_HPP

#include <string>

namespace vast {
namespace util {
namespace detail {

/// Constructs a UNIX domain socket.
/// @param path The file system path where to construct the socket.
/// @returns The descriptor of the domain socket on success or -1 on failure.
int uds_listen(std::string const& path);

/// Accepts a UNIX domain socket.
/// @param socket The file descriptor created with ::uds_listen.
/// @returns The accepted file descriptor or <0 on failure.
int uds_accept(int socket);

/// Connects to UNIX domain socket.
/// @param path The file system path where to the existing domain socket.
/// @returns The descriptor of the domain socket on success or -1 on failure.
int uds_connect(std::string const& path);

/// Sends a file descriptor over a UNIX domain socket.
/// @param socket The domain socket descriptor.
/// @param fd The file descriptor to send.
/// @returns `true` on success.
bool uds_send_fd(int socket, int fd);

/// Receives a file descriptor from a UNIX domain socket.
/// @param socket The domain socket descriptor.
/// @returns A file descriptor or -1 on failure.
int uds_recv_fd(int socket);

} // namespace detail
} // namespace util
} // namespace vast

#endif
