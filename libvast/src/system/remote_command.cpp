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

#include "vast/detail/narrow.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/system/connect_to_node.hpp"

using namespace std::chrono_literals;

namespace vast::system {

caf::message remote_command(const command& cmd, caf::actor_system& sys,
                            caf::settings& options,
                            command::argument_iterator first,
                            command::argument_iterator last) {
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", first, last));
  // Get a convenient and blocking way to interact with actors.
  caf::scoped_actor self{sys};
  // Get VAST node.
  auto node_opt = connect_to_node(self, options);
  if (!node_opt)
    return caf::make_message(std::move(node_opt.error()));
  auto node = std::move(*node_opt);
  self->monitor(node);
  // Delegate command to node.
  std::vector<std::string> argv;
  argv.reserve(detail::narrow_cast<size_t>(std::distance(first, last) + 1));
  argv.emplace_back(cmd.name.begin(), cmd.name.end());
  argv.insert(argv.end(), first, last);
  caf::error err;
  self->send(node, std::move(argv), options);
  self->receive(
    [&](const caf::down_msg&) {
      err = ec::remote_node_down;
    },
    [&](caf::ok_atom) {
      // Standard reply for success.
    },
    [&](caf::actor&) {
      // "vast spawn" returns an actor.
    },
    [&](const std::string& str) {
      // Status messages or query results.
      std::cout << str << std::endl;
    },
    [&](caf::error& e) {
      err = std::move(e);
    }
  );
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
