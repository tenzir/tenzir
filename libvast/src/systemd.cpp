#include "vast/systemd.hpp"

#include "vast/detail/env.hpp"
#include "vast/detail/posix.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"

#include <caf/detail/scope_guard.hpp>
#include <sys/socket.h>

#include <cstdlib>
#include <cstring>

namespace vast::systemd {

#if VAST_LINUX

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

// Integration with systemd only makes sense on linux.
caf::error notify_ready() {
  return caf::none;
}

#endif // VAST_LINUX

} // namespace vast::systemd
