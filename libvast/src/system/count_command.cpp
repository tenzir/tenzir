//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/count_command.hpp"

#include "vast/fwd.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/read_query.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/start_command.hpp"

#include <caf/actor.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>

#include <chrono>
#include <csignal>

using namespace caf;
using namespace std::chrono_literals;

namespace vast::system {

caf::message count_command(const invocation& inv, caf::actor_system& sys) {
  VAST_DEBUG("{}", inv);
  const auto& options = inv.options;
  // Read query from input file, STDIN or CLI arguments.
  auto query = read_query(inv, "vast.count.read", must_provide_query::no);
  if (!query)
    return caf::make_message(std::move(query.error()));
  // Get a convenient and blocking way to interact with actors.
  caf::scoped_actor self{sys};
  // Get VAST node.
  auto node_opt
    = system::spawn_or_connect_to_node(self, options, content(sys.config()));
  if (auto err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  auto local_node = !std::holds_alternative<node_actor>(node_opt);
  const auto& node = local_node
                       ? std::get<scope_linked<node_actor>>(node_opt).get()
                       : std::get<node_actor>(node_opt);
  VAST_ASSERT(node != nullptr);
  if (local_node) {
    // Register as the termination handler.
    auto signal_reflector
      = sys.registry().get<signal_reflector_actor>("signal-reflector");
    self->send(signal_reflector, atom::subscribe_v);
  }
  // Spawn COUNTER at the node.
  caf::actor cnt;
  auto args = invocation{options, "spawn counter", {*query}};
  VAST_DEBUG("{} spawns counter with parameters: {}",
             detail::pretty_type_name(inv.full_name), query);
  auto err = caf::error{};
  self->request(node, caf::infinite, atom::spawn_v, std::move(args))
    .receive(
      [&](caf::actor actor) {
        cnt = std::move(actor);
        if (!cnt)
          err = caf::make_error(ec::invalid_result, //
                                "remote spawn returned nullptr");
      },
      [&](caf::error e) { //
        err = std::move(e);
      });
  if (err)
    return caf::make_message(std::move(err));
  self->send(cnt, atom::run_v, self);
  bool counting = true;
  uint64_t result = 0;
  self->receive_while
    // Loop until false.
    (counting)
    // Message handlers.
    (
      [&](uint64_t x) {
        result += x;
      },
      [&](atom::done) {
        counting = false;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} got {}", detail::pretty_type_name(inv.full_name),
                   ::strsignal(signal));
        VAST_ASSERT(signal == SIGINT || signal == SIGTERM);
        counting = false;
      });
  std::cout << result << std::endl;
  return {};
}

} // namespace vast::system
