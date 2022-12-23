//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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

#include <csignal>
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
  if (auto err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node = std::holds_alternative<node_actor>(node_opt)
                       ? std::get<node_actor>(node_opt)
                       : std::get<scope_linked<node_actor>>(node_opt).get();
  self->monitor(node);
  // Delegate invocation to node.
  caf::error err = caf::none;
  self->send(node, atom::run_v, std::move(inv));
  self->receive(
    [&](const caf::down_msg&) {
      err = ec::remote_node_down;
    },
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
    [&](caf::error e) {
      err = std::move(e);
    },
    [&](atom::signal, int signal) {
      VAST_DEBUG("{} received signal {}", __PRETTY_FUNCTION__,
                 ::strsignal(signal));
      VAST_ASSERT(signal == SIGINT || signal == SIGTERM);
    });
  if (err)
    return caf::make_message(std::move(err));
  return {};
}

} // namespace vast::system
