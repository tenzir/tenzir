#include "vast/systemd.hpp"

#include "vast/concept/parseable/numeric.hpp"
#include "vast/detail/env.hpp"
#include "vast/detail/posix.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <caf/detail/scope_guard.hpp>
#include <sys/socket.h>
#include <sys/stat.h>

#include <cstdlib>
#include <cstring>

namespace vast::systemd {

#if VAST_LINUX

bool connected_to_journal() {
  auto journal_env = detail::locked_getenv("JOURNAL_STREAM");
  if (!journal_env)
    return false;
  size_t device_number = 0;
  char colon = 0;
  size_t inode_number = 0;
  auto parser = parsers::u64 >> parsers::ch<':'> >> parsers::u64;
  if (parser(*journal_env, device_number, colon, inode_number)) {
    VAST_WARN("could not parse systemd environment variable "
              "$JOURNAL_STREAM='{}'",
              *journal_env);
    return false;
  }
  struct stat buf = {};
  // TODO: errno-ify this
  auto stderrfd = ::fileno(stderr);
  ::fstat(stderrfd, &buf);
  return buf.st_dev == device_number && buf.st_ino == inode_number;
}

// This function implements the `sd_notify()` protocol to signal readyness
// to the service manager (systemd). This code follows the behaviour of the
// reference implementation at `libsystemd/sd-daemon/sd-daemon.c` to decide
// which conditions should result in errors.
caf::error notify_ready() {
  caf::detail::scope_guard guard([] {
    // Always unset $NOTIFY_SOCKET.
    if (auto result = detail::locked_unsetenv("NOTIFY_SOCKET"); !result)
      VAST_WARN("failed to unset NOTIFY_SOCKET: {}", result.error());
  });
  auto notify_socket_env = detail::locked_getenv("NOTIFY_SOCKET");
  if (!notify_socket_env)
    return caf::none;
  VAST_VERBOSE("notifying systemd at {}", *notify_socket_env);
  int socket = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (socket < 0)
    return caf::make_error(ec::system_error, "failed to create unix socket");
  if (detail::uds_sendmsg(socket, *notify_socket_env, "READY=1\n") < 0)
    return caf::make_error(ec::system_error, "failed to send ready message");
  return caf::none;
}

#else // !VAST_LINUX

bool connected_to_journal() {
  return false;
}

// Integration with systemd only makes sense on linux.
caf::error notify_ready() {
  return caf::none;
}

#endif // VAST_LINUX

} // namespace vast::systemd
