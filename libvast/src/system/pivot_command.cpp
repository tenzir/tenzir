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

#include "vast/system/pivot_command.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/accountant_actor.hpp"
#include "vast/system/make_sink.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/read_query.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"

#if VAST_ENABLE_PCAP
#  include "vast/format/pcap.hpp"
#endif

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <csignal>
#include <string>

using namespace std::chrono_literals;

namespace vast::system {

caf::message pivot_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE(inv);
  using namespace std::string_literals;
  // Read options and arguments.
  auto output_format = caf::get_or(inv.options, "vast.pivot.format", "json"s);
  // Read query from input file, STDIN or CLI arguments.
  auto query = read_query(inv, "vast.pivot.read", 1u);
  if (!query)
    return caf::make_message(std::move(query.error()));
  caf::actor sink;
  // Create sink for format.
  auto s = system::make_sink(sys, output_format, inv.options);
  if (!s)
    return make_message(s.error());
  sink = *s;
  caf::scoped_actor self{sys};
  auto sink_guard = caf::detail::make_scope_guard(
    [&] { self->send_exit(sink, caf::exit_reason::user_shutdown); });
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
                 ? caf::get<caf::actor>(node_opt)
                 : caf::get<scope_linked_actor>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  // Spawn exporter at the node.
  auto spawn_exporter = invocation{inv.options, "spawn exporter", {*query}};
  VAST_DEBUG(&inv, "spawns exporter with parameters:", spawn_exporter);
  auto exp = spawn_at_node(self, node, spawn_exporter);
  if (!exp)
    return caf::make_message(std::move(exp.error()));
  auto exp_guard = caf::detail::make_scope_guard(
    [&] { self->send_exit(*exp, caf::exit_reason::user_shutdown); });
  // Spawn pivoter at the node.
  auto spawn_pivoter
    = invocation{inv.options, "spawn pivoter", {inv.arguments[0], *query}};
  VAST_DEBUG(&inv, "spawns pivoter with parameters:", spawn_pivoter);
  auto piv = spawn_at_node(self, node, spawn_pivoter);
  if (!piv)
    return caf::make_message(std::move(piv.error()));
  auto piv_guard = caf::detail::make_scope_guard(
    [&] { self->send_exit(*piv, caf::exit_reason::user_shutdown); });
  // Register the accountant at the Sink.
  auto components = get_node_components(self, node, "accountant");
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto& [accountant] = *components;
  if (accountant) {
    VAST_DEBUG(inv.full_name, "assigns accountant to sink");
    self->send(sink, caf::actor_cast<accountant_actor>(accountant));
  }
  caf::error err;
  self->monitor(sink);
  self->monitor(*piv);
  // Start the exporter.
  self->send(*exp, atom::sink_v, *piv);
  // (Ab)use query_statistics as done message.
  self->send(*exp, atom::statistics_v, *piv);
  self->send(*piv, atom::sink_v, sink);
  self->send(*exp, atom::run_v);
  auto stop = false;
  self
    ->do_receive(
      [&](caf::down_msg& msg) {
        if (msg.source == node) {
          VAST_DEBUG(inv.full_name, "received DOWN from node");
        } else if (msg.source == *piv) {
          VAST_DEBUG(inv.full_name, "received DOWN from pivoter");
          piv_guard.disable();
        } else if (msg.source == sink) {
          VAST_DEBUG(inv.full_name, "received DOWN from sink");
          sink_guard.disable();
        } else {
          VAST_ASSERT(!"received DOWN from inexplicable actor");
        }
        if (msg.reason) {
          VAST_WARNING(inv.full_name, "received error message:",
                       self->system().render(msg.reason));
          err = std::move(msg.reason);
        }
        stop = true;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG(inv.full_name, "got", ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM) {
          stop = true;
        }
      })
    .until([&] { return stop; });
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
