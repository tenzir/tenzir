//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/explore_command.hpp"

#include "vast/defaults.hpp"
#include "vast/detail/saturating_arithmetic.hpp"
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
#include "vast/uuid.hpp"

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
  VAST_DEBUG("{}", inv);
  if (auto error = explorer_validate_args(inv.options))
    return make_message(error);
  // Read options and arguments.
  auto output_format = caf::get_or(inv.options, "vast.explore.format", "json"s);
  // Read query from input file, STDIN or CLI arguments.
  auto query = read_query(inv, "vast.export.read", must_provide_query::yes, 0);
  if (!query)
    return caf::make_message(std::move(query.error()));
  uint64_t max_events_query
    = caf::get_or(inv.options, "vast.explore.max-events-query",
                  defaults::explore::max_events_query);
  uint64_t max_events_context
    = caf::get_or(inv.options, "vast.explore.max-events-context",
                  defaults::explore::max_events_context);
  uint64_t max_events = caf::get_or(inv.options, "vast.explore.max-events",
                                    defaults::explore::max_events);
  // Get a local actor to interact with `sys`.
  caf::scoped_actor self{sys};
  auto alternative_max_events
    = detail::saturating_mul(max_events_query, max_events_context);
  if (alternative_max_events < max_events) {
    max_events = alternative_max_events;
    VAST_VERBOSE("{} adjusts max-events to {}", *self, max_events);
    caf::put(const_cast<caf::settings&>(inv.options), // NOLINT
             "vast.export.max-events", max_events);
  }
  auto s = make_sink(sys, output_format, inv.options);
  if (!s)
    return make_message(s.error());
  auto sink = *s;
  auto sink_guard = caf::detail::make_scope_guard([&] {
    VAST_DEBUG("{} sending exit to sink", *self);
    self->send_exit(sink, caf::exit_reason::user_shutdown);
  });
  self->monitor(sink);
  // Get VAST node.
  auto node_opt = system::spawn_or_connect_to_node(self, inv.options,
                                                   content(sys.config()));
  if (auto* err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node = std::holds_alternative<node_actor>(node_opt)
                       ? std::get<node_actor>(node_opt)
                       : std::get<scope_linked<node_actor>>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  // Spawn exporter for the passed query
  auto spawn_exporter = invocation{inv.options, "spawn exporter", {*query}};
  caf::put(spawn_exporter.options, "vast.export.max-events", max_events_query);
  VAST_DEBUG("{} spawns exporter with parameters: {}", inv, spawn_exporter);
  auto maybe_exporter = spawn_at_node(self, node, spawn_exporter);
  if (!maybe_exporter)
    return caf::make_message(std::move(maybe_exporter.error()));
  auto exporter = caf::actor_cast<exporter_actor>(std::move(*maybe_exporter));
  auto exporter_guard = caf::detail::make_scope_guard([&] {
    VAST_DEBUG("{} sending exit to exporter", *self);
    self->send_exit(exporter, caf::exit_reason::user_shutdown);
  });
  // Spawn explorer at the node.
  auto explorer_options = inv.options;
  auto spawn_explorer = invocation{explorer_options, "spawn explorer", {}};
  VAST_DEBUG("{} spawns explorer with parameters: {}", inv, spawn_explorer);
  auto explorer = spawn_at_node(self, node, spawn_explorer);
  if (!explorer)
    return caf::make_message(std::move(explorer.error()));
  auto explorer_guard = caf::detail::make_scope_guard([&] {
    VAST_DEBUG("{} sending exit to explorer", *self);
    self->send_exit(*explorer, caf::exit_reason::user_shutdown);
  });
  self->monitor(*explorer);
  // TODO: We want to be able to send the exporter_actor directly to the
  // explorer here, but that requires adding a type ID for exporter_actor. We
  // must re-think where we define typed actor interfaces to make them sendable
  // over the wire.
  self->send(*explorer, atom::provision_v, exporter);
  // Set the explorer as sink for the initial query exporter.
  self->send(exporter, atom::sink_v, *explorer);
  // (Ab)use query_statistics as done message.
  self->send(exporter, atom::statistics_v, *explorer);
  self->send(*explorer, atom::sink_v, sink);
  self->send(exporter, atom::run_v);
  caf::error err;
  auto stop = false;
  self
    ->do_receive(
      [&](caf::down_msg& msg) {
        if (msg.source == node) {
          VAST_DEBUG("{} received DOWN from node", inv.full_name);
        } else if (msg.source == *explorer) {
          VAST_DEBUG("{} received DOWN from explorer", inv.full_name),
            explorer_guard.disable();
        } else if (msg.source == sink) {
          VAST_DEBUG("{} received DOWN from sink", inv.full_name);
          sink_guard.disable();
        } else {
          VAST_ASSERT(!"received DOWN from inexplicable actor");
        }
        if (msg.reason) {
          VAST_DEBUG("{} received error message: {}", inv.full_name,
                     msg.reason);
          err = std::move(msg.reason);
        }
        stop = true;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} got  {}", detail::pretty_type_name(inv.full_name),
                   ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM) {
          stop = true;
        }
      })
    .until([&] {
      return stop;
    });
  if (err)
    return caf::make_message(std::move(err));
  return {};
}

} // namespace vast::system
