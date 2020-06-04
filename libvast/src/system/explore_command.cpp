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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/make_io_stream.hpp"
#include "vast/error.hpp"
#include "vast/format/json.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/read_query.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/spawn_explorer.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/start_command.hpp"
#include "vast/system/tracker.hpp"

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

caf::message
explore_command(const command::invocation& invocation, caf::actor_system& sys) {
  VAST_DEBUG_ANON(invocation);
  const auto& options = invocation.options;
  if (auto error = explorer_validate_args(invocation.options))
    return make_message(error);
  // Read query from input file, STDIN or CLI arguments.
  auto query = read_query(invocation, "export.read", 0);
  if (!query)
    return caf::make_message(std::move(query.error()));
  size_t max_events_search = caf::get_or(options, "explore.max-events-query",
                                         defaults::explore::max_events_query);
  // Get a local actor to interact with `sys`.
  caf::scoped_actor self{sys};
  // TODO: Add --format option to select output format.
  auto out = detail::make_output_stream(
    std::string{format::json::writer::defaults::write}, false);
  if (!out)
    return make_message(out.error());
  caf::actor writer
    = sys.spawn(sink<format::json::writer>,
                format::json::writer{std::move(*out)}, max_events);
  if (!writer)
    return make_message(ec::unspecified, "could not spawn writer");
  auto writer_guard = caf::detail::make_scope_guard([&] {
    VAST_DEBUG(self, "sending exit to writer");
    self->send_exit(writer, caf::exit_reason::user_shutdown);
  });
  self->monitor(writer);
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
  auto spawn_exporter
    = command::invocation{invocation.options, "spawn exporter", {*query}};
  if (max_events_search)
    caf::put(spawn_exporter.options, "export.max-events", max_events_search);
  VAST_DEBUG(&invocation, "spawns exporter with parameters:", spawn_exporter);
  auto exporter = spawn_at_node(self, node, spawn_exporter);
  if (!exporter)
    return caf::make_message(std::move(exporter.error()));
  auto exporter_guard = caf::detail::make_scope_guard([&] {
    VAST_DEBUG(self, "sending exit to exporter");
    self->send_exit(*exporter, caf::exit_reason::user_shutdown);
  });
  // Spawn explorer at the node.
  auto explorer_options = invocation.options;
  auto spawn_explorer
    = command::invocation{explorer_options, "spawn explorer", {}};
  VAST_DEBUG(&invocation, "spawns explorer with parameters:", spawn_explorer);
  auto explorer = spawn_at_node(self, node, spawn_explorer);
  if (!explorer)
    return caf::make_message(std::move(explorer.error()));
  auto explorer_guard = caf::detail::make_scope_guard([&] {
    VAST_DEBUG(self, "sending exit to explorer");
    self->send_exit(*explorer, caf::exit_reason::user_shutdown);
  });
  self->monitor(*explorer);
  self->send(*explorer, provision_atom::value, *exporter);
  // Set the explorer as sink for the initial query exporter.
  self->send(*exporter, system::sink_atom::value, *explorer);
  // (Ab)use query_statistics as done message.
  self->send(*exporter, system::statistics_atom::value, *explorer);
  self->send(*explorer, system::sink_atom::value, writer);
  self->send(*exporter, system::run_atom::value);
  caf::error err;
  auto stop = false;
  self
    ->do_receive(
      [&](caf::down_msg& msg) {
        if (msg.source == node) {
          VAST_DEBUG(invocation.full_name, "received DOWN from node");
        } else if (msg.source == *explorer) {
          VAST_DEBUG(invocation.full_name, "received DOWN from explorer");
          explorer_guard.disable();
        } else if (msg.source == writer) {
          VAST_DEBUG(invocation.full_name, "received DOWN from sink");
          writer_guard.disable();
        } else {
          VAST_ASSERT(!"received DOWN from inexplicable actor");
        }
        if (msg.reason) {
          VAST_DEBUG(invocation.full_name, "received error message:",
                     self->system().render(msg.reason));
          err = std::move(msg.reason);
        }
        stop = true;
      },
      [&](system::signal_atom, int signal) {
        VAST_DEBUG(invocation.full_name, "got " << ::strsignal(signal));
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
