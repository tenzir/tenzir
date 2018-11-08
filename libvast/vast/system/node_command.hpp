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

#include <caf/expected.hpp>

#include "vast/command.hpp"
#include "vast/scope_linked.hpp"

namespace vast::system {

/// A command that starts or runs on a VAST node.
class node_command : public command {
public:
  // -- member types -----------------------------------------------------------

  using node_factory_result
    = caf::variant<caf::error, caf::actor, scope_linked_actor>;

  // -- constructors, destructors, and assignment operators --------------------

  explicit node_command(command* parent);

  ~node_command();

  /// Either spawns a new VAST node or connects to a server, depending on the
  /// configuration.
  node_factory_result
  spawn_or_connect_to_node(caf::scoped_actor& self,
                           const caf::config_value_map& opts);

  /// Spawns a new VAST node.
  caf::expected<scope_linked_actor>
  spawn_node(caf::scoped_actor& self, const caf::config_value_map& opts);

  /// Connects to a remote VAST server.
  caf::expected<caf::actor> connect_to_node(caf::scoped_actor& self,
                                            const caf::config_value_map& opts);
};

} // namespace vast

