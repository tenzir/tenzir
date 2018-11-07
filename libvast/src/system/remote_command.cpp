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

#include <caf/scoped_actor.hpp>

#include <iostream>

#include "vast/logger.hpp"

using namespace caf;

namespace vast::system {
using namespace std::chrono_literals;

remote_command::remote_command(command* parent) : node_command(parent) {
  // nop
}

caf::message remote_command::run_impl(actor_system& sys,
                                      const caf::config_value_map& options,
                                      argument_iterator begin,
                                      argument_iterator end) {
  VAST_TRACE(VAST_ARG("name", name()), VAST_ARG(options),
             VAST_ARG("args", begin, end));
  // Get a convenient and blocking way to interact with actors.
  scoped_actor self{sys};
  // Get VAST node.
  auto node_opt = connect_to_node(self, options);
  if (!node_opt)
    return wrap_error(std::move(node_opt.error()));
  auto node = std::move(*node_opt);
  self->monitor(node);
  // Build command to remote node.
  auto args = caf::message_builder{begin, end}.move_to_message();
  auto cmd = make_message(std::string{name()}, std::move(args));
  // Delegate command to node.
  caf::error err;
  self->send(node, std::move(cmd));
  self->receive(
    [&](const down_msg&) {
      err = ec::remote_node_down;
    },
    [&](ok_atom) {
      // Standard reply for success.
    },
    [&](actor&) {
      // "vast spawn" returns an actor.
    },
    [&](const std::string& str) {
      // Status messages or query results.
      std::cout << str << std::endl;
    },
    [&](error& e) {
      err = std::move(e);
    }
  );
  if (err)
    return wrap_error(std::move(err));
  return caf::none;
}

} // namespace vast::system
