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

#include <caf/fwd.hpp>

#include "vast/fwd.hpp"

namespace vast::system {

/// Either spawns a new VAST node or connects to a server, depending on the
/// configuration.
caf::variant<caf::error, caf::actor, scope_linked<caf::actor>>
spawn_or_connect_to_node(caf::scoped_actor& self,
                         const caf::config_value_map& opts);

} // namespace vast::system

