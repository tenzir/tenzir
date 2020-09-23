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

#include "vast/system/explore_command.hpp"

#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/make_sink.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/read_query.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/spawn_explorer.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"

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

caf::message explore_command(const invocation& inv, caf::actor_system& sys) {
  using namespace std::string_literals;
  VAST_DEBUG_ANON(inv);
  const auto& options = inv.options;
  if (auto error = explorer_validate_args(inv.options))
    return make_message(error);
  // Read options and arguments.
  auto output_format = caf::get_or(inv.options, "vast.explore.format", "json"s);
  // Read query from input file, STDIN or CLI arguments.
  auto query = read_query(inv, "vast.export.read", 0);
  if (!query)
    return caf::make_message(std::move(query.error()));
  size_t max_events_search
    = caf::get_or(options, "vast.explore.max-events-query",
                  defaults::explore::max_events_query);
  // Get a local actor to interact with `sys`.
  caf::scoped_actor self{sys};
  auto s = make_sink(sys, output_format, inv.options);
  if (!s)
    return make_message(s.error());
  auto sink = *s;
  auto sink_guard = caf::detail::make_scope_guard([&] {
    VAST_DEBUG(self, "sending exit to sink");
    self->send_exit(sink, caf::exit_reason::user_shutdown);
  });
  self->monitor(sink);
  // Get VAST node.
  auto node_opt
    = system::spawn_or_connect_to_node(self, options, content(sys.config()));
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
  // Spawn exporter for the passed query
  auto spawn_exporter = invocation{inv.options, "spawn exporter", {*query}};
  if (max_events_search)
    caf::put(spawn_exporter.options, "vast.export.max-events",
             max_events_search);
  VAST_DEBUG(&inv, "spawns exporter with parameters:", spawn_exporter);
  auto exporter = spawn_at_node(self, node, spawn_exporter);
  if (!exporter)
    return caf::make_message(std::move(exporter.error()));
  auto exporter_guard = caf::detail::make_scope_guard([&] {
    VAST_DEBUG(self, "sending exit to exporter");
    self->send_exit(*exporter, caf::exit_reason::user_shutdown);
  });
  // Spawn explorer at the node.
  auto explorer_options = inv.options;
  auto spawn_explorer = invocation{explorer_options, "spawn explorer", {}};
  VAST_DEBUG(&inv, "spawns explorer with parameters:", spawn_explorer);
  auto explorer = spawn_at_node(self, node, spawn_explorer);
  if (!explorer)
    return caf::make_message(std::move(explorer.error()));
  auto explorer_guard = caf::detail::make_scope_guard([&] {
    VAST_DEBUG(self, "sending exit to explorer");
    self->send_exit(*explorer, caf::exit_reason::user_shutdown);
  });
  self->monitor(*explorer);
  self->send(*explorer, atom::provision_v, *exporter);
  // Set the explorer as sink for the initial query exporter.
  self->send(*exporter, atom::sink_v, *explorer);
  // (Ab)use query_statistics as done message.
  self->send(*exporter, atom::statistics_v, *explorer);
  self->send(*explorer, atom::sink_v, sink);
  self->send(*exporter, atom::run_v);
  caf::error err;
  auto stop = false;
  self
    ->do_receive(
      [&](caf::down_msg& msg) {
        if (msg.source == node) {
          VAST_DEBUG(inv.full_name, "received DOWN from node");
        } else if (msg.source == *explorer) {
          VAST_DEBUG(inv.full_name, "received DOWN from explorer");
          explorer_guard.disable();
        } else if (msg.source == sink) {
          VAST_DEBUG(inv.full_name, "received DOWN from sink");
          sink_guard.disable();
        } else {
          VAST_ASSERT(!"received DOWN from inexplicable actor");
        }
        if (msg.reason) {
          VAST_DEBUG(inv.full_name, "received error message:",
                     self->system().render(msg.reason));
          err = std::move(msg.reason);
        }
        stop = true;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG(inv.full_name, "got ", ::strsignal(signal));
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
