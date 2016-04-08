#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>

#include <cstring>

#include "vast/config.hpp"
#include "vast/util/detail/posix.hpp"

namespace vast {
namespace util {
namespace detail {

int uds_listen(std::string const& path) {
  int fd;
  if ((fd = ::socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
    return fd;
  ::sockaddr_un un;
  std::memset(&un, 0, sizeof(un));
  un.sun_family = AF_UNIX;
  std::strncpy(un.sun_path, path.data(), sizeof(un.sun_path));
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

int uds_connect(std::string const& path) {
  int fd;
  if ((fd = ::socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
    return fd;
  ::sockaddr_un un;
  std::memset(&un, 0, sizeof(un));
  un.sun_family = AF_UNIX;
  std::strncpy(un.sun_path, path.data(), sizeof(un.sun_path));
  if (::connect(fd, reinterpret_cast<sockaddr*>(&un), sizeof(un)) < 0)
    return -1;
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
  m.msg_name = NULL;
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

} // namespace detail
} // namespace util
} // namespace vast
