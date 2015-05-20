#include <fcntl.h>
#include <unistd.h>
#include <sys/select.h>

#include <cassert>
#include <cerrno>
#include <stdexcept>

#include "vast/util/posix.h"
#include "vast/util/detail/posix.h"

namespace vast {
namespace util {

int unix_domain_socket::listen(std::string const& path)
{
  return detail::uds_listen(path);
}

unix_domain_socket unix_domain_socket::accept(std::string const& path)
{
  auto server = detail::uds_listen(path);
  if (server != -1)
    return unix_domain_socket{detail::uds_accept(server)};
  return unix_domain_socket{};
}

unix_domain_socket unix_domain_socket::connect(std::string const& path)
{
  return unix_domain_socket{detail::uds_connect(path)};
}

unix_domain_socket::unix_domain_socket(int fd)
  : fd_{fd}
{
}

unix_domain_socket::operator bool() const
{
  return fd_ != -1;
}

bool unix_domain_socket::send_fd(int fd)
{
  assert(*this);
  return detail::uds_send_fd(fd_, fd);
}

int unix_domain_socket::recv_fd()
{
  assert(*this);
  return detail::uds_recv_fd(fd_);
}

int unix_domain_socket::fd() const
{
  return fd_;
}

namespace {

bool make_nonblocking(int fd, bool flag)
{
  auto flags = ::fcntl(fd, F_GETFL, 0);
  if (flags == -1)
    return false;
  flags = flag ? flags | O_NONBLOCK : flags & ~O_NONBLOCK;
  return ::fcntl(fd, F_SETFL, flags) != -1;
}

} // namespace <anonymous>

bool make_nonblocking(int fd)
{
  return make_nonblocking(fd, true);
}

bool make_blocking(int fd)
{
  return make_nonblocking(fd, false);
}

bool poll(int fd, int usec)
{
  fd_set rdset;
  FD_ZERO(&rdset);
  FD_SET(fd, &rdset);
  timeval timeout{0, usec};
  auto rc = ::select(fd + 1, &rdset, nullptr, nullptr, &timeout);
  if (rc < 0)
  {
    switch (rc)
    {
      default:
        throw std::logic_error("unhandled select() error");
      case EINTR:
      case ENOMEM:
        return false;
    }
  }
  return FD_ISSET(fd, &rdset);
}

bool close(int fd)
{
  int result;
  do
  {
    result = ::close(fd);
  }
  while (result < 0 && errno == EINTR);
  return result == 0;
}

bool read(int fd, void* buffer, size_t bytes, size_t* got)
{
  ssize_t taken;
  do
  {
    taken = ::read(fd, buffer, bytes);
  }
  while (taken < 0 && errno == EINTR);
  if (taken <= 0) // EOF == 0, error == -1
    return false;
  if (got)
    *got = static_cast<size_t>(taken);
  return true;
}

bool write(int fd, void const* buffer, size_t bytes, size_t* put)
{
  auto total = size_t{0};
  auto buf = reinterpret_cast<uint8_t const*>(buffer);
  while (total < bytes)
  {
    ssize_t written;
    do
    {
      written = ::write(fd, buf + total, bytes - total);
    }
    while (written < 0 && errno == EINTR);
    if (written <= 0)
      return false;
    total += static_cast<size_t>(written);
  }
  if (put)
    *put = total;
  return true;
}

bool seek(int fd, size_t bytes)
{
  return ::lseek(fd, bytes, SEEK_CUR) != -1;
}

} // namespace util
} // namespace vast
