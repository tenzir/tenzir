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

#include "vast/system/remote_command.hpp"

#include "vast/atoms.hpp"
#include "vast/command.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"

#include <caf/actor.hpp>
#include <caf/scoped_actor.hpp>

#include <iostream>

using namespace std::chrono_literals;

namespace vast::system {

caf::message remote_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE_SCOPE("{}", inv);
  // Get a convenient and blocking way to interact with actors.
  caf::scoped_actor self{sys};
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  auto& node = caf::holds_alternative<node_actor>(node_opt)
                 ? caf::get<node_actor>(node_opt)
                 : caf::get<scope_linked<node_actor>>(node_opt).get();
  self->monitor(node);
  // Delegate invocation to node.
  caf::error err = caf::none;
  self->send(node, atom::run_v, std::move(inv));
  self->receive([&](const caf::down_msg&) { err = ec::remote_node_down; },
                [&](atom::ok) {
                  // Standard reply for success.
                },
                [&](caf::actor&) {
                  // "vast spawn" returns an actor.
                },
                [&](const std::string& str) {
                  // Status messages or query results.
                  std::cout << str << std::endl;
                },
                [&](caf::error e) { err = std::move(e); });
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
