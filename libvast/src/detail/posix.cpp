//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/detail/posix.hpp"

#include "vast/config.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/raise_error.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <caf/expected.hpp>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <stdexcept>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

namespace vast::detail {

auto describe_errno(int err) -> std::string {
  auto result = std::string(256, '\0');
  if (strerror_r(err, result.data(), result.size()) != 0) {
    return fmt::format("<errno = {}>", err);
  }
  result.erase(std::find(result.begin(), result.end(), '\0'), result.end());
  return result;
}

int uds_listen(const std::string& path) {
  int fd;
  if ((fd = ::socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
    return fd;
  ::sockaddr_un un;
  std::memset(&un, 0, sizeof(un));
  un.sun_family = AF_UNIX;
  std::strncpy(un.sun_path, path.data(), sizeof(un.sun_path) - 1);
  ::unlink(path.c_str()); // Always remove previous socket file.
  auto sa = reinterpret_cast<sockaddr*>(&un);
  if (::bind(fd, sa, sizeof(un)) < 0 || ::listen(fd, 10) < 0) {
    ::close(fd);
    return -1;
  }
  return fd;
}

int uds_accept(int socket) {
  if (socket < 0)
    return -1;
  int fd;
  ::sockaddr_un un;
  socklen_t size = sizeof(un);
  if ((fd = ::accept(socket, reinterpret_cast<::sockaddr*>(&un), &size)) < 0)
    return -1;
  return fd;
}

uds_datagram_sender::uds_datagram_sender(uds_datagram_sender&& other) noexcept
  : src_fd{other.src_fd}, dst{other.dst} {
  // Invalidate the original copy to avoid closing when `other` is destroyed.
  other.src_fd = -1;
}

uds_datagram_sender&
uds_datagram_sender::operator=(uds_datagram_sender&& other) noexcept {
  src_fd = other.src_fd;
  // Invalidate the original copy to avoid closing when `other` is destroyed.
  other.src_fd = -1;
  dst = other.dst;
  return *this;
}

uds_datagram_sender::~uds_datagram_sender() {
  if (src_fd != -1)
    ::close(src_fd);
}

caf::expected<uds_datagram_sender>
uds_datagram_sender::make(const std::string& path) {
  auto result = uds_datagram_sender{};
  result.src_fd = ::socket(AF_UNIX, SOCK_DGRAM, 0);
  if (result.src_fd < 0)
    return caf::make_error(
      ec::system_error,
      "failed to obtain an AF_UNIX DGRAM socket: ", ::strerror(errno));
  if (auto err = make_nonblocking(result.src_fd))
    return err;
  // Create a unique temporary directory for a place to bind the sending side
  // to. There is no mktemp variant for sockets, so that is unfortunately
  // necessary.
  // NOTE: The temporary directory will be removed at the end of this function.
  char mkd_template[] = "/tmp/vast-XXXXXX\0socket";
  char* src_name = ::mkdtemp(&mkd_template[0]);
  if (src_name == nullptr)
    return caf::make_error(ec::system_error,
                           fmt::format("failed in mkdtemp({}): {}",
                                       mkd_template, ::strerror(errno)));
  // Replace the first null terminator with a directory separator to get the
  // full path.
  src_name[16] = '/';
  ::sockaddr_un src = {};
  std::memset(&src, 0, sizeof(src));
  src.sun_family = AF_UNIX;
  std::strncpy(src.sun_path, src_name, sizeof(src.sun_path) - 1);
  if (::bind(result.src_fd, reinterpret_cast<sockaddr*>(&src), sizeof(src)) < 0)
    return caf::make_error(ec::system_error,
                           "failed to bind client socket:", ::strerror(errno));
  // From https://man7.org/linux/man-pages/man2/unlink.2.html:
  //   If the name was the last link to a file but any processes still
  //   have the file open, the file will remain in existence until the
  //   last file descriptor referring to it is closed.
  // -> We can delegate the socket removal to the kernel by calling unlink on
  //    it right away.
  if (::unlink(src_name) != 0) {
    VAST_WARN("{} failed in unlink({}): {}", __func__, src_name,
              ::strerror(errno));
  } else {
    src_name[16] = '\0';
    if (::rmdir(src_name) != 0)
      VAST_WARN("{} failed in rmdir({}): {}", __func__, src_name,
                ::strerror(errno));
  }
  // Prepare the destination socket address.
  std::memset(&result.dst, 0, sizeof(result.dst));
  result.dst.sun_family = AF_UNIX;
  std::strncpy(result.dst.sun_path, path.data(),
               sizeof(result.dst.sun_path) - 1);
  return std::move(result);
}

caf::error uds_datagram_sender::send(std::span<char> data, int timeout_usec) {
  // We try sending directly before polling to only use a single system call in
  // the happy path.
  auto sent
    = ::sendto(src_fd, data.data(), data.size(), 0,
               reinterpret_cast<sockaddr*>(&dst), sizeof(struct sockaddr_un));
  if (sent == detail::narrow_cast<int>(data.size()))
    return caf::none;
  if (sent >= 0)
    return caf::make_error(ec::incomplete,
                           fmt::format("::sendto could only transmit {} of {} "
                                       "bytes in a single datagram",
                                       sent, data.size()));
  if (errno != EAGAIN && errno != EWOULDBLOCK)
    return caf::make_error(ec::system_error, "::sendto: ", ::strerror(errno));
  if (timeout_usec == 0)
    return ec::timeout;
  auto ready = wpoll(src_fd, timeout_usec);
  if (!ready)
    return ready.error();
  // We just attempt to send again instead of returning ec::timeout outright.
  // This handles the case when the receiving socket was replaced on the file
  // system, but the original one was kept alive with an open file descriptor.
  // The next send would go to the correct destination in that case.
  sent
    = ::sendto(src_fd, data.data(), data.size(), 0,
               reinterpret_cast<sockaddr*>(&dst), sizeof(struct sockaddr_un));
  if (sent == detail::narrow_cast<int>(data.size()))
    return caf::none;
  if (sent >= 0)
    return caf::make_error(ec::incomplete,
                           fmt::format("::sendto could only transmit {} of {} "
                                       "bytes in a single datagram",
                                       sent, data.size()));
  if (errno != EAGAIN && errno != EWOULDBLOCK)
    return caf::make_error(ec::system_error, "::sendto: ", ::strerror(errno));
  return ec::timeout;
}

int uds_connect(const std::string& path, socket_type type) {
  int fd{};
  switch (type) {
    case socket_type::stream:
    case socket_type::fd:
      if ((fd = ::socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
        return fd;
      break;
    case socket_type::datagram:
      if ((fd = ::socket(AF_UNIX, SOCK_DGRAM, 0)) < 0)
        return fd;
      ::sockaddr_un clt;
      std::memset(&clt, 0, sizeof(clt));
      clt.sun_family = AF_UNIX;
      auto client_path = path + "-client";
      std::strncpy(clt.sun_path, client_path.data(), sizeof(clt.sun_path) - 1);
      ::unlink(client_path.c_str()); // Always remove previous socket file.
      if (::bind(fd, reinterpret_cast<sockaddr*>(&clt), sizeof(clt)) < 0) {
        VAST_WARN("{} failed in bind: {}", __func__, ::strerror(errno));
        return -1;
      }
      break;
  }
  ::sockaddr_un srv;
  std::memset(&srv, 0, sizeof(srv));
  srv.sun_family = AF_UNIX;
  std::strncpy(srv.sun_path, path.data(), sizeof(srv.sun_path) - 1);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&srv), sizeof(srv)) < 0) {
    if (!(type == socket_type::datagram && errno == ENOENT)) {
      VAST_WARN("{} failed in connect: {}", __func__, ::strerror(errno));
      return -1;
    }
  }
  return fd;
}

// On Mac OS, CMSG_SPACE is for some reason not a constant expression.
VAST_DIAGNOSTIC_PUSH
VAST_DIAGNOSTIC_IGNORE_VLA_EXTENSION

bool uds_send_fd(int socket, int fd) {
  if (socket < 0)
    return -1;
  char dummy = '*';
  ::iovec iov[1];
  iov[0].iov_base = &dummy;
  iov[0].iov_len = sizeof(dummy);
  char ctrl_buf[CMSG_SPACE(sizeof(int))];
  std::memset(ctrl_buf, 0, CMSG_SPACE(sizeof(int)));
  // Setup message header.
  ::msghdr m;
  std::memset(&m, 0, sizeof(struct msghdr));
  m.msg_name = nullptr;
  m.msg_namelen = 0;
  m.msg_iov = iov;
  m.msg_iovlen = 1;
  m.msg_controllen = CMSG_SPACE(sizeof(int));
  m.msg_control = ctrl_buf;
  // Setup control message header.
  auto c = CMSG_FIRSTHDR(&m);
  c->cmsg_level = SOL_SOCKET;
  c->cmsg_type = SCM_RIGHTS;
  c->cmsg_len = CMSG_LEN(sizeof(int));
  *reinterpret_cast<int*>(CMSG_DATA(c)) = fd;
  // Send a message.
  return ::sendmsg(socket, &m, 0) > 0;
}

int uds_recv_fd(int socket) {
  if (socket < 0)
    return -1;
  char ctrl_buf[CMSG_SPACE(sizeof(int))];
  std::memset(ctrl_buf, 0, CMSG_SPACE(sizeof(int)));
  char dummy;
  ::iovec iov[1];
  iov[0].iov_base = &dummy;
  iov[0].iov_len = sizeof(dummy);
  // Setup message header.
  ::msghdr m;
  std::memset(&m, 0, sizeof(struct msghdr));
  m.msg_name = nullptr;
  m.msg_namelen = 0;
  m.msg_control = ctrl_buf;
  m.msg_controllen = CMSG_SPACE(sizeof(int));
  m.msg_iov = iov;
  m.msg_iovlen = 1;
  // Receive a message.
  if (::recvmsg(socket, &m, 0) <= 0)
    return -1;
  // Iterate over control message headers until we find the descriptor.
  for (auto c = CMSG_FIRSTHDR(&m); c != nullptr; c = CMSG_NXTHDR(&m, c))
    if (c->cmsg_level == SOL_SOCKET && c->cmsg_type == SCM_RIGHTS)
      return *reinterpret_cast<int*>(CMSG_DATA(c));
  return -1;
}

VAST_DIAGNOSTIC_POP

int uds_sendmsg(int socket, const std::string& destination,
                const std::string& msg, int flags) {
  struct sockaddr_un dst;
  std::memset(&dst, 0, sizeof(dst));
  if (destination.empty() || destination.size() >= sizeof(dst.sun_path))
    return -EINVAL;
  dst.sun_family = AF_UNIX;
  std::strncpy(dst.sun_path, destination.data(), sizeof(dst.sun_path) - 1);
  struct iovec iovec;
  std::memset(&iovec, 0, sizeof(iovec));
  iovec.iov_base = const_cast<char*>(msg.data());
  iovec.iov_len = msg.size();
  struct msghdr msghdr;
  std::memset(&msghdr, 0, sizeof(msghdr));
  msghdr.msg_name = &dst;
  // For abstract domain sockets (i.e., where the first char is '@'), the
  // terminating NUL byte is not counted towards the length, but any NUL
  // bytes embedded in the path are. For example, in the extreme case it
  // would be possible to have two different sockets named "\0" and "\0\0".
  auto pathlen = destination[0] == '@' ? destination.size()
                                       : std::strlen(&destination[0]) + 1;
  msghdr.msg_namelen = offsetof(struct sockaddr_un, sun_path) + pathlen;
  msghdr.msg_iov = &iovec;
  msghdr.msg_iovlen = 1;
  return ::sendmsg(socket, &msghdr, flags);
}

unix_domain_socket unix_domain_socket::listen(const std::string& path) {
  return unix_domain_socket{detail::uds_listen(path)};
}

unix_domain_socket unix_domain_socket::accept(const std::string& path) {
  auto server = detail::uds_listen(path);
  if (server != -1)
    return unix_domain_socket{detail::uds_accept(server)};
  return unix_domain_socket{};
}

unix_domain_socket
unix_domain_socket::connect(const std::string& path, socket_type type) {
  return unix_domain_socket{detail::uds_connect(path, type)};
}

unix_domain_socket::operator bool() const {
  return fd != -1;
}

bool unix_domain_socket::send_fd(int fd) {
  VAST_ASSERT(*this);
  return detail::uds_send_fd(this->fd, fd);
}

int unix_domain_socket::recv_fd() {
  VAST_ASSERT(*this);
  return detail::uds_recv_fd(fd);
}

namespace {

[[nodiscard]] caf::error make_nonblocking(int fd, bool flag) {
  auto flags = ::fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return caf::make_error(ec::filesystem_error,
                           "failed in fcntl(2):", std::strerror(errno));
  flags = flag ? flags | O_NONBLOCK : flags & ~O_NONBLOCK;
  if (::fcntl(fd, F_SETFL, flags) == -1)
    return caf::make_error(ec::filesystem_error,
                           "failed in fcntl(2):", std::strerror(errno));
  return caf::none;
}

} // namespace

caf::error make_nonblocking(int fd) {
  return make_nonblocking(fd, true);
}

caf::error make_blocking(int fd) {
  return make_nonblocking(fd, false);
}

caf::expected<bool> rpoll(int fd, int usec) {
  fd_set read_set;
  FD_ZERO(&read_set);
  FD_SET(fd, &read_set);
  timeval timeout{0, usec};
  auto rc = ::select(fd + 1, &read_set, nullptr, nullptr, &timeout);
  if (rc < 0)
    return caf::make_error(ec::filesystem_error,
                           "failed in select(2):", std::strerror(errno));
  return !!FD_ISSET(fd, &read_set);
}

caf::expected<bool> wpoll(int fd, int usec) {
  fd_set write_set;
  FD_ZERO(&write_set);
  FD_SET(fd, &write_set);
  timeval timeout{0, usec};
  auto rc = ::select(fd + 1, nullptr, &write_set, nullptr, &timeout);
  if (rc < 0)
    return caf::make_error(ec::filesystem_error,
                           "failed in select(2):", std::strerror(errno));
  return !!FD_ISSET(fd, &write_set);
}

caf::error close(int fd) {
  int result;
  do {
    result = ::close(fd);
  } while (result < 0 && errno == EINTR);
  if (result != 0)
    return caf::make_error(ec::filesystem_error,
                           "failed in close(2):", std::strerror(errno));
  return caf::none;
}

// Note: Enable the *large_file_io* test in filesystem.cpp to test
// modifications to this funciton.
caf::expected<size_t> read(int fd, void* buffer, size_t bytes) {
  auto total = size_t{0};
  auto buf = reinterpret_cast<uint8_t*>(buffer);
  while (total < bytes) {
    ssize_t taken;
    // On darwin (macOS), read returns with EINVAL if more than INT_MAX bytes
    // are requested. This problem might also exist on other platforms, so we
    // defensively limit our calls everywhere.
    constexpr size_t read_max = std::numeric_limits<int>::max();
    auto request_size = std::min(read_max, bytes - total);
    do {
      taken = ::read(fd, buf + total, request_size);
    } while (taken < 0 && errno == EINTR);
    if (taken < 0) // error
      return caf::make_error(ec::filesystem_error,
                             "failed in read(2):", std::strerror(errno));
    if (taken == 0) // EOF
      break;
    total += static_cast<size_t>(taken);
  }
  return total;
}

// Note: Enable the *large_file_io* test in filesystem.cpp to test
// modifications to this funciton.
caf::expected<size_t> write(int fd, const void* buffer, size_t bytes) {
  auto total = size_t{0};
  auto buf = reinterpret_cast<const uint8_t*>(buffer);
  while (total < bytes) {
    ssize_t written;
    // On darwin (macOS), write returns with EINVAL if more than INT_MAX bytes
    // are requested. This problem might also exist on other platforms, so we
    // defensively limit our calls everywhere.
    constexpr size_t write_max = std::numeric_limits<int>::max();
    auto request_size = std::min(write_max, bytes - total);
    do {
      written = ::write(fd, buf + total, request_size);
    } while (written < 0 && errno == EINTR);
    if (written < 0)
      return caf::make_error(ec::filesystem_error,
                             "failed in write(2):", std::strerror(errno));
    // write should not return 0 if it wasn't asked to write that amount. We
    // want to cover this case anyway in case it ever happens.
    if (written == 0)
      return caf::make_error(ec::filesystem_error, "write(2) returned 0");
    total += static_cast<size_t>(written);
  }
  return total;
}

caf::error seek(int fd, size_t bytes) {
  if (::lseek(fd, bytes, SEEK_CUR) == -1)
    return caf::make_error(ec::filesystem_error,
                           "failed in seek(2):", std::strerror(errno));
  return caf::none;
}

} // namespace vast::detail
