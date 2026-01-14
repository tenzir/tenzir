#include "tenzir/systemd.hpp"

#include "tenzir/concept/parseable/numeric.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/detail/posix.hpp"
#include "tenzir/detail/scope_guard.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"

#include <sys/socket.h>
#include <sys/stat.h>

#include <cstdlib>
#include <cstring>
#include <iostream>

namespace tenzir::systemd {

#if TENZIR_LINUX

bool connected_to_journal() {
  auto journal_env = detail::getenv("JOURNAL_STREAM");
  if (! journal_env) {
    return false;
  }
  size_t device_number = 0;
  size_t inode_number = 0;
  auto parser = parsers::u64 >> ':' >> parsers::u64;
  if (! parser(*journal_env, device_number, inode_number)) {
    // Can't use TENZIR_WARN() here, because this is called as part
    // of the logger setup.
    std::cerr << "could not parse systemd environment variable "
                 "$JOURNAL_STREAM="
              << *journal_env << std::endl;
    return false;
  }
  // Most linux processes have bogus 'JOURNAL_STREAM' values in their
  // environment because some parent was writing to the journal at some
  // point, so we don't print errors in this case.
  auto stderrfd = ::fileno(stderr);
  if (stderrfd < 0) {
    return false;
  }
  struct stat buf = {};
  if (::fstat(stderrfd, &buf) == -1) {
    return false;
  }
  return buf.st_dev == device_number && buf.st_ino == inode_number;
}

// This function implements the `sd_notify()` protocol to signal readyness
// to the service manager (systemd). This code follows the behaviour of the
// reference implementation at `libsystemd/sd-daemon/sd-daemon.c` to decide
// which conditions should result in errors.
caf::error notify_ready() {
  detail::scope_guard guard([]() noexcept {
    // Always unset $NOTIFY_SOCKET.
    if (auto err = detail::unsetenv("NOTIFY_SOCKET"); err.valid()) {
      TENZIR_WARN("failed to unset NOTIFY_SOCKET: {}", err);
    }
  });
  auto notify_socket_env = detail::getenv("NOTIFY_SOCKET");
  if (! notify_socket_env) {
    return caf::none;
  }
  TENZIR_VERBOSE("notifying systemd at {}", *notify_socket_env);
  int socket = ::socket(AF_UNIX, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (socket < 0) {
    return caf::make_error(ec::system_error, "failed to create unix socket");
  }
  if (detail::uds_sendmsg(socket, notify_socket_env->data(), "READY=1\n") < 0) {
    return caf::make_error(ec::system_error, "failed to send ready message");
  }
  return caf::none;
}

#else // !TENZIR_LINUX

bool connected_to_journal() {
  return false;
}

// Integration with systemd only makes sense on linux.
caf::error notify_ready() {
  return caf::none;
}

#endif // TENZIR_LINUX

} // namespace tenzir::systemd
