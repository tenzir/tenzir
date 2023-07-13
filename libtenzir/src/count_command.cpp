//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/count_command.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/error.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/read_query.hpp"
#include "tenzir/scope_linked.hpp"
#include "tenzir/spawn_or_connect_to_node.hpp"

#include <caf/actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include <chrono>
#include <csignal>

using namespace std::chrono_literals;

namespace tenzir {

caf::message count_command(const invocation& inv, caf::actor_system& sys) {
  TENZIR_DEBUG("{}", inv);
  const auto& options = inv.options;
  // Read query from input file, STDIN or CLI arguments.
  auto query = read_query(inv, "tenzir.count.read", must_provide_query::no);
  if (!query)
    return caf::make_message(std::move(query.error()));
  // Get a convenient and blocking way to interact with actors.
  caf::scoped_actor self{sys};
  // Get Tenzir node.
  auto node_opt
    = spawn_or_connect_to_node(self, options, content(sys.config()));
  if (auto err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node = std::holds_alternative<node_actor>(node_opt)
                       ? std::get<node_actor>(node_opt)
                       : std::get<scope_linked<node_actor>>(node_opt).get();
  TENZIR_ASSERT(node != nullptr);
  // Spawn COUNTER at the node.
  caf::actor cnt;
  auto args = invocation{options, "spawn counter", {*query}};
  TENZIR_DEBUG("{} spawns counter with parameters: {}",
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
        TENZIR_DEBUG("{} got {}", detail::pretty_type_name(inv.full_name),
                     ::strsignal(signal));
        TENZIR_ASSERT(signal == SIGINT || signal == SIGTERM);
        counting = false;
      });
  std::cout << result << std::endl;
  return {};
}

} // namespace tenzir
