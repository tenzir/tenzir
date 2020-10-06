#pragma once

#include <caf/error.hpp>

namespace vast::systemd {

/// Use the sd_notify() protocol to signal readyness to the service manager.
/// Also unsets the environment variable `NOTIFY_SOCKET` unconditionally, so
/// subsequent invocations will do nothing and always return success.
///
/// Does nothing if this VAST process is not running under systemd supervision,
/// or was not set up to send status updates by setting `Type=notify` in the
/// service definition.
///
/// NOTE: This function is not thread-safe, since it modifies the global
/// environment by unsetting the variable NOTIFY_SOCKET if it exists.
caf::error notify_ready();

} // namespace vast::systemd