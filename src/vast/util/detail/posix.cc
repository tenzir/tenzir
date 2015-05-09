#include "vast/util/detail/posix.h"

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

namespace vast {
namespace util {
namespace detail {

int uds_listen(std::string const& path)
{
  int fd;
  if ((fd = ::socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
    return fd;
  ::sockaddr_un un;
  std::memset(&un, 0, sizeof(un));
  un.sun_family = AF_LOCAL;
  std::strncpy(un.sun_path, path.data(), sizeof(un.sun_path));
  // Remove previous socket file.
  ::unlink(path.c_str());
  auto size = offsetof(sockaddr_un, sun_path) + std::strlen(un.sun_path);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&un), size) < 0
      || ::listen(fd, 10) < 0)
  {
    ::close(fd);
    return -1;
  }
  return fd;
}

int uds_accept(int socket)
{
  if (socket < 0)
    return -1;
  int fd;
  ::sockaddr_un un;
  socklen_t size = sizeof(un);
  if ((fd = ::accept(socket, reinterpret_cast<::sockaddr*>(&un), &size)) < 0)
    return -1;
  return fd;
}

int uds_connect(std::string const& path)
{
  int fd;
  if ((fd = ::socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
    return fd;
  ::sockaddr_un un;
  std::memset(&un, 0, sizeof(un));
  un.sun_family = AF_LOCAL;
  std::strncpy(un.sun_path, path.data(), sizeof(un.sun_path));
  auto size = offsetof(sockaddr_un, sun_path) + std::strlen(un.sun_path);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&un), size) < 0)
    return -1;
  return fd;
}

bool uds_send_fd(int socket, int fd)
{
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

int uds_recv_fd(int socket)
{
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

} // namespace detail
} // namespace util
} // namespace vast
