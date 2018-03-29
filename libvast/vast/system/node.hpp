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

#include <string>

#include "vast/filesystem.hpp"

#include "vast/system/node_state.hpp"

namespace vast::system {

/// Spawns a node.
/// @param self The actor handle
/// @param id The unique ID of the node.
/// @param dir The directory where to store persistent state.
caf::behavior node(caf::stateful_actor<node_state>* self, std::string id,
                   path dir);

} // namespace vast::system

