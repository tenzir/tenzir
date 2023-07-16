//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/remote_command.hpp"

#include "tenzir/atoms.hpp"
#include "tenzir/command.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/scope_linked.hpp"
#include "tenzir/spawn_or_connect_to_node.hpp"

#include <caf/actor.hpp>
#include <caf/scoped_actor.hpp>

#include <csignal>
#include <iostream>

using namespace std::chrono_literals;

namespace tenzir {

caf::message remote_command(const invocation& inv, caf::actor_system& sys) {
  TENZIR_TRACE_SCOPE("{}", inv);
  // Get a convenient and blocking way to interact with actors.
  caf::scoped_actor self{sys};
  // Get Tenzir node.
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
      // "tenzir spawn" returns an actor.
    },
    [&](const std::string& str) {
      // Status messages or query results.
      std::cout << str << std::endl;
    },
    [&](caf::error e) {
      err = std::move(e);
    },
    [&](atom::signal, int signal) {
      TENZIR_DEBUG("{} received signal {}", __PRETTY_FUNCTION__,
                   ::strsignal(signal));
      TENZIR_ASSERT(signal == SIGINT || signal == SIGTERM);
    });
  if (err)
    return caf::make_message(std::move(err));
  return {};
}

} // namespace tenzir
