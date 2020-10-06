#include "vast/socket_activation.hpp"

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository: https://gitbox.apache.org/repos/asf/mesos
// - Commit:     d6b26b367b294aca43ff2d28c50293886ad1d5d4
// - Path:       src/linux/systemd.cpp
// - Author:     Benno Evers
// - Copyright:  Licensed by the Apache Foundation
// - License:    Apache 2.0

#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <climits>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <unordered_set>
#include <vector>

namespace {

static caf::error set_cloexec(int fd) {
  VAST_ASSERT(fd >= 0);
  int flags = ::fcntl(fd, F_GETFD, 0);
  if (flags < 0)
    return caf::make_error(vast::ec::system_error, "failed to get flags for fd"
                                                     + std::to_string(fd) + ": "
                                                     + strerror(errno));
  int nflags = flags | FD_CLOEXEC;
  if (nflags == flags)
    return caf::none;
  if (::fcntl(fd, F_SETFD, nflags) < 0)
    return caf::make_error(vast::ec::system_error,
                           "failed to set CLOEXEC flag on fd"
                             + std::to_string(fd) + ": " + strerror(errno));
  return caf::none;
}

} // namespace

namespace vast::socket_activation {

/// Defined in `man(3) sd_listen_fds`.
constexpr static int SD_LISTEN_FDS_START = 3;

// See `src/libsystemd/sd-daemon/sd-daemon.c` in the systemd source tree
// for the reference implementation. We follow that implementation to
// decide which conditions should result in errors and which should return
// an empty array.
caf::expected<std::vector<int>> listen_fds() {
  using namespace std::string_literals;
  std::vector<int> result;
  const char* listen_pid_env = ::getenv("LISTEN_PID");
  if (!listen_pid_env)
    return result;
  pid_t listen_pid;
  if (!parsers::u64(std::string_view{listen_pid_env}, listen_pid))
    return make_error(ec::format_error, "could not parse $LISTEN_PID=\""s
                                          + listen_pid_env + "\" as integer");
  pid_t pid = ::getpid();
  if (listen_pid != pid) {
    VAST_WARNING_ANON("Socket activation file descriptors were passed for pid",
                      listen_pid, ", ignoring them because we have pid", pid);
    return result;
  }
  const char* listen_fds_env = ::getenv("LISTEN_FDS");
  if (!listen_fds_env)
    return result;
  int listen_fds;
  if (!parsers::i64(std::string_view{listen_pid_env}, listen_fds))
    return make_error(ec::format_error, "could not parse $LISTEN_FDS=\""s
                                          + listen_fds_env + "\" as integer");
  int n = listen_fds.get();
  if (n <= 0 || n > INT_MAX - SD_LISTEN_FDS_START)
    return make_error(ec::invalid_configuration, "too many passed file "
                                                 "descriptors");
  for (int fd = SD_LISTEN_FDS_START; fd < SD_LISTEN_FDS_START + n; ++fd)
    if (auto error = set_cloexec(fd))
      return error;
  result.resize(n);
  std::iota(result.begin(), result.begin() + n, SD_LISTEN_FDS_START);
  return result;
}

caf::expected<std::vector<int>>
listen_fds_with_names(const std::unordered_set<std::string_view>& names) {
  auto fds = listen_fds();
  if (!fds)
    return fds.error();
  std::vector<std::string_view> listen_fdnames;
  const char* listen_fdnames_env = ::getenv("LISTEN_FDNAMES");
  if (listen_fdnames_env) {
    listen_fdnames = detail::split(listen_fdnames_env, ":");
    if (listen_fdnames.size() != fds->size())
      return make_error(ec::format_error, "size mismatch between file "
                                          "descriptors and names");
  } else {
    // If `LISTEN_FDNAMES` is not set in the environment, libsystemd assigns
    // the special name "unknown" to all passed file descriptors.
    // We do the same.
    listen_fdnames.resize(fds->size());
    std::fill_n(listen_fdnames.begin(), listen_fdnames.size(), "unknown");
  }
  std::vector<int> result;
  for (size_t i = 0; i < listen_fdnames.size(); ++i)
    if (names.count(listen_fdnames[i]))
      result.push_back(fds->at(i));
  return result;
}

void clear_environment() {
  ::unsetenv("LISTEN_PID");
  ::unsetenv("LISTEN_FDS");
  ::unsetenv("LISTEN_FDNAMES");
}

} // namespace vast::socket_activation