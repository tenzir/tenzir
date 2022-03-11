//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/system/actors.hpp"

#include <caf/typed_actor.hpp>

namespace vast::system {

/// Checks if `opts` contains a valid configuration for an EXPLORER.
caf::error explorer_validate_args(const caf::settings& opts);

/// Assigns default arguments to explorer-related fields of `opts`.
void explorer_assign_defaults(caf::settings& opts);

/// Tries to spawn a new EXPLORER.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @returns a handle to the spawned actor on success, an error otherwise
caf::expected<caf::actor>
spawn_explorer(node_actor::stateful_pointer<node_state> self,
               spawn_arguments& args);

} // namespace vast::system
