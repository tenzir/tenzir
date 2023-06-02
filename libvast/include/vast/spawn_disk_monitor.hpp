//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/actors.hpp"

#include <caf/typed_actor.hpp>

namespace vast {

// TODO: This is temporary until the functionality is unified with
// the ERASER.

/// Tries to spawn a new DISK_MONITOR.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
caf::expected<caf::actor>
spawn_disk_monitor(node_actor::stateful_pointer<node_state> self,
                   spawn_arguments& args);

} // namespace vast
