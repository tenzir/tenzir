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
