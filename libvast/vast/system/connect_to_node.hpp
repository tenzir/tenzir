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

#include <caf/config_value.hpp>
#include <caf/expected.hpp>
#include <caf/fwd.hpp>

#include "vast/scope_linked.hpp"

namespace vast::system {

/// Connects to a remote VAST server.
caf::expected<caf::actor> connect_to_node(caf::scoped_actor& self,
                                          const caf::settings& opts);

} // namespace vast

