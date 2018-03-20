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

#include "vast/system/run_remote.hpp"

#include <iostream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#ifdef VAST_USE_OPENSSL
#include <caf/openssl/all.hpp>
#endif // VAST_USE_OPENSSL

#include "vast/logger.hpp"

#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn.hpp"

using namespace caf;

namespace vast::system {
using namespace std::chrono_literals;

run_remote::run_remote(command* parent, std::string_view name)
  : base_command(parent, name) {
  // nop
}

int run_remote::run_impl(actor_system& sys, opt_map& options, message args) {
  CAF_LOG_TRACE(CAF_ARG2("name", name()) << CAF_ARG(options) << CAF_ARG(args));
  // Get a convenient and blocking way to interact with actors.
  scoped_actor self{sys};
  // Get VAST node.
  auto node_opt = connect_to_node(self, options);
  if (!node_opt) {
    std::cerr << "unable to connect to node: " << sys.render(node_opt.error())
              << std::endl;
    return EXIT_FAILURE;
  }
  auto node = std::move(*node_opt);
  // Build command to remote node.
  auto cmd = make_message(std::string{name()}) + args;
  // Delegate command to node.
  auto result = true;
  self->send(node, std::move(cmd), std::move(args));
  self->receive(
    [&](const down_msg& msg) {
      if (msg.reason != exit_reason::user_shutdown) {
        std::cerr << "remote node down" << std::endl;
        result = false;
      }
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
    [&](const error& e) {
      VAST_IGNORE_UNUSED(e);
      VAST_ERROR(self->system().render(e));
      std::cerr << self->system().render(e) << std::endl;
      result = false;
    });
  return result ? 0 : 1;
}

} // namespace vast::system
