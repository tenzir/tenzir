//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/system/actors.hpp"

#include <caf/typed_actor.hpp>

namespace vast::system {

/// Tries to spawn a new IMPORTER.
/// @param self Points to the parent actor.
/// @param args Configures the new actor.
/// @param transforms Input transformations to be applied.
/// @returns a handle to the spawned actor on success, an error otherwise
caf::expected<caf::actor>
spawn_importer(node_actor::stateful_pointer<node_state> self,
               spawn_arguments& args);

} // namespace vast::system
