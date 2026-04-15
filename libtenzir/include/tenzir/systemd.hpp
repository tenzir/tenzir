//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <caf/error.hpp>

namespace tenzir::systemd {

/// Checks if the binary was set up to write its log output to journald,
/// e.g. by using the `StandardError=journal` directive.
bool connected_to_journal();

/// Use the sd_notify() protocol to signal readyness to the service manager.
/// Also unsets the environment variable `NOTIFY_SOCKET` unconditionally, so
/// subsequent invocations will do nothing and always return success.
///
/// Does nothing if this Tenzir process is not running under systemd
/// supervision, or was not set up to send status updates by setting
/// `Type=notify` in the service definition.
///
/// NOTE: This function is not thread-safe, since it modifies the global
/// environment by unsetting the variable NOTIFY_SOCKET if it exists.
caf::error notify_ready();

} // namespace tenzir::systemd
