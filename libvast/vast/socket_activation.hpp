/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository: https://gitbox.apache.org/repos/asf/mesos
// - Commit:     d6b26b367b294aca43ff2d28c50293886ad1d5d4
// - Path:       src/linux/systemd.hpp
// - Author:     Benno Evers
// - Copyright:  Licensed by the Apache Foundation
// - License:    Apache 2.0

#pragma once

#include <unordered_set>
#include <vector>

#include "caf/expected.hpp"

namespace vast::socket_activation {

// A re-implementation of the systemd socket activation API.
//
// To implement the socket-passing protocol, systemd uses the
// environment variables `$LISTEN_PID`, `$LISTEN_FDS` and `$LISTEN_FDNAMES`
// according to the scheme documented in [1], [2].
//
// Users of libsystemd can use the following API to interface
// with the socket passing functionality:
//
//     #include <systemd/sd-daemon.h>
//     int sd_listen_fds(int unset_environment);
//     int sd_listen_fds_with_names(int unset_environment, char ***names);
//
// The `sd_listen_fds()` function does the following:
//
//  * The return value is the number of listening sockets passed by
//    systemd. The actual file descriptors of these sockets are
//    numbered 3...n+3.
//  * If the current pid is different from the one specified by the
//    environment variable $LISTEN_PID, 0 is returned
//  * The `CLOEXEC` option will be set on all file descriptors "returned"
//    by this function.
//  * If `unset_environment` is true, the environment variables $LISTEN_PID,
//    $LISTEN_FDS, $LISTEN_FDNAMES will be cleared.
//
// The `sd_listen_fds_with_names()` function additionally does the following:
//
//  * If $LISTEN_FDS is set, will return an array of strings with the
//    names. By default, the name of a socket will be equal to the
//    name of the unit file containing the socket description.
//  * The special string "unknown" is used for sockets where no name
//    could be determined.
//
// For this reimplementation, the interface was slightly changed to better
// suit the needs of the VAST codebase. However, we still set the `CLOEXEC`
// flag on all file descriptors passed via socket activation when one of
// these functions is called.
//
// [1] https://www.freedesktop.org/software/systemd/man/sd_listen_fds.html#Notes
// [2] http://0pointer.de/blog/projects/socket-activation.html

caf::expected<std::vector<int>> listen_fds();

// The names are set by the `FileDescriptorName=` directive in the unit file.
// This requires systemd 227 or newer. Since any number of unit files can
// specify the same name, this can return more than one file descriptor.
caf::expected<std::vector<int>>
listen_fds_with_names(const std::unordered_set<std::string_view>& names);

// Clear the `$LISTEN_PID`, `$LISTEN_FDS` and `$LISTEN_FDNAMES` environment
// variables.
//
// NOTE: This function is not thread-safe, since it modifies the global
// environment.
void clear_environment();

} // namespace vast::socket_activation