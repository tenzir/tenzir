//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/fwd.hpp>
#include <sys/socket.h>
#include <sys/un.h>

#include <span>
#include <string>

/// Various POSIX-compliant helper tools.

namespace vast::detail {

// Returns a textual representation for `errno`. In contrast to `strerror`, this
// function is thread-safe.
auto describe_errno(int err = errno) -> std::string;

enum class socket_type { datagram, stream, fd };

/// Holds the necessary state to send unix datagrams to a destination socket.
struct uds_datagram_sender {
  uds_datagram_sender() = default;

  uds_datagram_sender(const uds_datagram_sender&) = delete;
  uds_datagram_sender(uds_datagram_sender&&) noexcept;

  uds_datagram_sender operator=(const uds_datagram_sender&) = delete;
  uds_datagram_sender& operator=(uds_datagram_sender&&) noexcept;

  /// The destructor closes the `src_fd`.
  ~uds_datagram_sender();

  /// Helper function to create a sender.
  static caf::expected<uds_datagram_sender> make(const std::string& path);

  /// Send the content of `data to `dst`.
  /// @param data The data that should be written.
  /// @param timeout_usec A duration to wait for readiness of the receiver.
  /// @returns `no_error` if the data was sent or `timeout` if it was dropped
  ///          because the timeout was reached
  caf::error send(std::span<char> data, int timeout_usec = 0);

  /// The file descriptor for the "client" socket.
  int src_fd = -1;

  /// The socket address object for the destination.
  ::sockaddr_un dst;
};

/// Constructs a UNIX domain socket.
/// @param path The file system path where to construct the socket.
/// @returns The descriptor of the domain socket on success or -1 on failure.
int uds_listen(const std::string& path);

/// Accepts a UNIX domain socket.
/// @param socket The file descriptor created with ::uds_listen.
/// @returns The accepted file descriptor or <0 on failure.
int uds_accept(int socket);

/// Connects to UNIX domain socket.
/// @param path The file system path where to the existing domain socket.
/// @param type The socket type.
/// @returns The descriptor of the domain socket on success or -1 on failure.
int uds_connect(const std::string& path, socket_type type);

/// Sends a single message over a UNIX domain socket.
/// @param socket The domain socket descriptor.
/// @param destination The destination to which to send the message.
/// @param msg The message payload
/// @param flags The flags are passed verbatim to `sendmsg(3)`.
/// @returns The number of bytes sent or -1 on failure.
int uds_sendmsg(int socket, const std::string& destination,
                const std::string& msg, int flags = 0);

/// Sends a file descriptor over a UNIX domain socket.
/// @param socket The domain socket descriptor.
/// @param fd The file descriptor to send.
/// @returns `true` on success.
bool uds_send_fd(int socket, int fd);

/// Receives a file descriptor from a UNIX domain socket.
/// @param socket The domain socket descriptor.
/// @returns A file descriptor or -1 on failure.
int uds_recv_fd(int socket);

/// An abstraction of a UNIX domain socket. This class facilitates sending and
/// receiving file descriptors.
struct [[nodiscard]] unix_domain_socket {
  /// Creates a UNIX domain socket listening server at a given path.
  /// @param path The filesystem path where to construct the socket.
  /// @returns A UNIX domain socket handle.
  static unix_domain_socket listen(const std::string& path);

  /// Creates a UNIX domain socket server and blocks to accept a connection.
  /// @param path The filesystem path where to construct the socket.
  /// @returns A UNIX domain socket handle.
  static unix_domain_socket accept(const std::string& path);

  /// Creates a UNIX domain socket client by connecting to an existing server.
  /// @param path The filesystem path identifying the server socket.
  /// @param type The socket type.
  /// @returns A UNIX domain socket handle.
  static unix_domain_socket
  connect(const std::string& path, socket_type type = socket_type::stream);

  /// Checks whether the UNIX domain socket is in working state.
  /// @returns `true` if the UNIX domain socket is open and operable.
  [[nodiscard]] explicit operator bool() const;

  /// Sends a file descriptor over the UNIX domain socket.
  /// @param fd The file descriptor to send.
  /// @pre `*this == true` and *fd* must be open.
  [[nodiscard]] bool send_fd(int fd);

  /// Receives a file descriptor from the UNIX domain socket.
  /// @returns The file descriptor from the other end.
  /// @pre `*this == true`.
  [[nodiscard]] int recv_fd();

  /// The file descriptor to the socket; defaults to -1, an invalid descriptor.
  const int fd = -1;
};

/// Puts a file descriptor into non-blocking mode.
/// @param fd The file descriptor to adjust.
/// @returns `caf::none` on success.
[[nodiscard]] caf::error make_nonblocking(int fd);

/// Puts a file descriptor into blocking mode.
/// @param fd The file descriptor to adjust.
/// @returns `caf::none` on success.
[[nodiscard]] caf::error make_blocking(int fd);

/// Polls a file descriptor for ready read events via `select(2)`.
/// @param fd The file descriptor to poll
/// @param usec The number of microseconds to wait.
/// @returns `true` if read operations on *fd* would not block.
[[nodiscard]] caf::expected<bool> rpoll(int fd, int usec);

/// Polls a file descriptor for write readiness via `select(2)`.
/// @param fd The file descriptor to poll
/// @param usec The number of microseconds to wait.
/// @returns `true` if write operations on *fd* would not block.
[[nodiscard]] caf::expected<bool> wpoll(int fd, int usec);

/// Wraps `close(2)`.
/// @param fd The file descriptor to close.
/// @returns `caf::none` on successful closing.
[[nodiscard]] caf::error close(int fd);

/// Wraps `read(2)`.
/// @param fd The file descriptor to read from.
/// @param buffer The buffer to write into.
/// @param bytes The number of bytes to read from *fd* and write into *buffer*.
/// @returns the number of bytes on successful reading.
[[nodiscard]] caf::expected<size_t> read(int fd, void* buffer, size_t bytes);

/// Wraps `write(2)`.
/// @param fd The file descriptor to write to.
/// @param buffer The buffer to read from.
/// @param bytes The number of bytes to write into *fd* from *buffer*.
/// @returns the number of written bytes on successful writing.
[[nodiscard]] caf::expected<size_t>
write(int fd, const void* buffer, size_t bytes);

/// Wraps `seek(2)`.
/// @param fd A seekable file descriptor.
/// @param bytes The number of bytes that should be skipped.
/// @returns `caf::none` on successful seeking.
[[nodiscard]] caf::error seek(int fd, size_t bytes);

} // namespace vast::detail
