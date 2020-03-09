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

#include "vast/system/sink_command.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/query_status.hpp"
#include "vast/system/read_query.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/tracker.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>

using namespace std::chrono_literals;
using namespace caf;

namespace vast::system {

caf::message sink_command(const command::invocation& invocation,
                          actor_system& sys, caf::actor snk) {
  // Get a convenient and blocking way to interact with actors.
  scoped_actor self{sys};
  auto guard = caf::detail::make_scope_guard(
    [&] { self->send_exit(snk, exit_reason::user_shutdown); });
  // Read query from input file, STDIN or CLI arguments.
  auto query = read_query(invocation, "export.read");
  if (!query)
    return caf::make_message(std::move(query.error()));
  // Transform expression if needed, e.g., for PCAP sink.
  if (invocation.name() == "pcap") {
    VAST_DEBUG(invocation.full_name, "restricts expression to PCAP packets");
    // We parse the query expression first, work on the AST, and then render
    // the expression again to avoid performing brittle string manipulations.
    auto expr = to<expression>(*query);
    if (!expr)
      return make_message(expr.error());
    auto attr = caf::atom_from_string("type");
    auto extractor = attribute_extractor{attr};
    auto pred = predicate{extractor, equal, data{"pcap.packet"}};
    auto ast = conjunction{std::move(pred), std::move(*expr)};
    *query = to_string(ast);
    VAST_DEBUG(&invocation, "transformed expression to", *query);
  }
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, invocation.options, content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
               ? caf::get<caf::actor>(node_opt)
               : caf::get<scope_linked_actor>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto signal_guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  auto spawn_exporter
    = command::invocation{invocation.options, "spawn exporter", {*query}};
  VAST_DEBUG(&invocation, "spawns exporter with parameters:", spawn_exporter);
  auto exp = spawn_at_node(self, node, spawn_exporter);
  if (!exp)
    return caf::make_message(std::move(exp.error()));
  // Register the accountant at the Sink.
  auto components = get_node_components(self, node, {"accountant"});
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto& [accountant] = *components;
  if (accountant) {
    VAST_DEBUG(invocation.full_name, "assigns accountant to new sink");
    self->send(snk, actor_cast<accountant_type>(accountant));
  }
  // Start the exporter.
  self->send(*exp, system::sink_atom::value, snk);
  self->send(*exp, system::run_atom::value);
  // Register self as the statistics actor.
  self->send(*exp, system::statistics_atom::value, self);
  self->send(snk, system::statistics_atom::value, self);
  self->monitor(snk);
  self->monitor(*exp);
  guard.disable();
  caf::error err;
  auto waiting_for_final_report = false;
  auto stop = false;
  self
    ->do_receive(
      [&](down_msg& msg) {
        stop = true;
        if (msg.source == node) {
          VAST_DEBUG_ANON(__func__, "received DOWN from node");
          self->send_exit(snk, exit_reason::user_shutdown);
          self->send_exit(*exp, exit_reason::user_shutdown);
        } else if (msg.source == *exp) {
          VAST_DEBUG(invocation.full_name, "received DOWN from exporter");
          self->send_exit(snk, exit_reason::user_shutdown);
        } else if (msg.source == snk) {
          VAST_DEBUG(invocation.full_name, "received DOWN from sink");
          self->send_exit(*exp, exit_reason::user_shutdown);
          stop = false;
          waiting_for_final_report = true;
        } else {
          VAST_ASSERT(!"received DOWN from inexplicable actor");
        }
        if (msg.reason && msg.reason != exit_reason::user_shutdown) {
          VAST_WARNING(invocation.full_name, "received error message:",
                       self->system().render(msg.reason));
          err = std::move(msg.reason);
        }
      },
      [&](performance_report report) {
        // Log a set of named measurements.
        VAST_DEBUG(invocation.full_name, "received performance report");
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
        for (const auto& [name, measurement] : report) {
          if (auto rate = measurement.rate_per_sec(); std::isfinite(rate))
            VAST_INFO(name, "processed", measurement.events,
                      "events at a rate of", static_cast<uint64_t>(rate),
                      "events/sec in", to_string(measurement.duration));
          else
            VAST_INFO(name, "processed", measurement.events, "events");
        }
#endif
      },
      [&](std::string name, query_status query) {
        // Log the query status.
        VAST_DEBUG(invocation.full_name, "received query status from", name);
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
        if (auto rate
            = measurement{query.runtime, query.processed}.rate_per_sec();
            std::isfinite(rate))
          VAST_INFO(name, "processed", query.processed,
                    "candidates at a rate of", static_cast<uint64_t>(rate),
                    "candidates/sec and shipped", query.shipped, "results in",
                    to_string(query.runtime));
        else
          VAST_INFO(name, "processed", query.processed, "candidates",
                    "and shipped", query.shipped, "results in",
                    to_string(query.runtime));
#endif
        if (waiting_for_final_report)
          stop = true;
      },
      [&](system::signal_atom, int signal) {
        VAST_DEBUG(invocation.full_name, "got " << ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM) {
          self->send_exit(*exp, exit_reason::user_shutdown);
          self->send_exit(snk, exit_reason::user_shutdown);
        }
      })
    .until([&] { return stop; });
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
