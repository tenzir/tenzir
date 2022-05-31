//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/sink_command.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/query_status.hpp"
#include "vast/system/read_query.hpp"
#include "vast/system/report.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/uuid.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <csignal>
#include <iostream>
#include <memory>
#include <string>

using namespace std::chrono_literals;

namespace vast::system {

caf::message
sink_command(const invocation& inv, caf::actor_system& sys, caf::actor snk) {
  // Get a convenient and blocking way to interact with actors.
  caf::scoped_actor self{sys};
  exporter_actor exporter = {};
  auto guard = caf::detail::make_scope_guard([&] {
    // Try to shut down the sink and the exporter, if they're still alive.
    self->send_exit(snk, caf::exit_reason::user_shutdown);
    self->send_exit(exporter, caf::exit_reason::user_shutdown);
  });
  self->monitor(snk);
  // Read query from input file, STDIN or CLI arguments.
  auto query = read_query(inv, "vast.export.read", must_provide_query::no);
  if (!query)
    return caf::make_message(std::move(query.error()));
  // Transform expression if needed, e.g., for PCAP sink.
  // TODO: Can we remove this special-casing, or move it to the PCAP plugin
  // somehow?
  if (inv.name() == "pcap") {
    VAST_DEBUG("{} restricts expression to PCAP packets",
               detail::pretty_type_name(inv.full_name));
    // We parse the query expression first, work on the AST, and then render
    // the expression again to avoid performing brittle string manipulations.
    auto expr = to<expression>(*query);
    if (!expr)
      return make_message(expr.error());
    auto pred = predicate{selector{selector::type}, relational_operator::equal,
                          data{"pcap.packet"}};
    auto ast = conjunction{std::move(pred), std::move(*expr)};
    *query = to_string(ast);
    VAST_DEBUG("{} transformed expression to {}", inv, *query);
  }
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node = std::holds_alternative<node_actor>(node_opt)
                       ? std::get<node_actor>(node_opt)
                       : std::get<scope_linked<node_actor>>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto signal_guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  auto spawn_exporter = invocation{inv.options, "spawn exporter", {*query}};
  VAST_DEBUG("{} spawns exporter with parameters: {}", inv, spawn_exporter);
  auto maybe_exporter = spawn_at_node(self, node, spawn_exporter);
  if (!maybe_exporter)
    return caf::make_message(std::move(maybe_exporter.error()));
  exporter = caf::actor_cast<exporter_actor>(std::move(*maybe_exporter));
  // Link ourselves to the exporter until we know that the exporter monitors us
  // to avoid a dead window on ungraceful exits where we leave dangling exporter
  // actors in the node.
  self->link_to(exporter);
  caf::error err = caf::none;
  self->request(exporter, caf::infinite, atom::sink_v, snk)
    .receive(
      [&]() {
        self->monitor(exporter);
        self->unlink_from(exporter);
      },
      [&](caf::error& error) {
        err = std::move(error);
      });
  if (err)
    return caf::make_message(std::move(err));
  // Register the accountant at the sink.
  auto components = get_node_components<accountant_actor>(self, node);
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto [accountant] = std::move(*components);
  if (accountant) {
    VAST_DEBUG("{} assigns accountant to new sink",
               detail::pretty_type_name(inv.full_name));
    self->send(snk, caf::actor_cast<accountant_actor>(accountant));
  }
  // Register self as the statistics actor.
  self->send(exporter, atom::statistics_v, self);
  self->send(snk, atom::statistics_v, self);
  // Start the exporter.
  self->send(exporter, atom::run_v);
  // Set the configured timeout, if any.
  if (auto timeout_str = caf::get_if<std::string>(&inv.options, //
                                                  "vast.export.timeout")) {
    if (auto timeout = to<duration>(*timeout_str))
      self->delayed_send(self, *timeout, atom::shutdown_v, *timeout);
    else
      VAST_ERROR("{} was unable parse timeout option {} as duration: {}",
                 inv.full_name, *timeout_str, timeout.error());
  }
  // Start the receive-loop.
  auto waiting_for_final_report = false;
  auto stop = false;
  self
    ->do_receive(
      [&](atom::shutdown, const duration& timeout) {
        VAST_INFO("{} shuts down after {} timeout", inv.full_name,
                  to_string(timeout));
        self->send_exit(exporter, caf::exit_reason::user_shutdown);
        self->send_exit(snk, caf::exit_reason::user_shutdown);
        waiting_for_final_report = true;
        err = caf::make_error(ec::timeout,
                              fmt::format("{} shut down after {} timeout",
                                          inv.full_name, to_string(timeout)));
      },
      [&, node_addr = node->address(), snk_addr = snk->address(),
       exporter_addr = exporter->address()](caf::down_msg& msg) {
        stop = true;
        if (msg.source == node_addr) {
          VAST_DEBUG("{} received DOWN from node", inv.full_name);
          self->send_exit(snk, caf::exit_reason::user_shutdown);
          self->send_exit(exporter, caf::exit_reason::user_shutdown);
          exporter = nullptr;
          snk = nullptr;
        } else if (msg.source == exporter_addr) {
          VAST_DEBUG("{} received DOWN from exporter", inv.full_name);
          self->send_exit(snk, caf::exit_reason::user_shutdown);
          exporter = nullptr;
          snk = nullptr;
        } else if (msg.source == snk_addr) {
          VAST_DEBUG("{} received DOWN from sink", inv.full_name);
          self->send_exit(exporter, caf::exit_reason::user_shutdown);
          exporter = nullptr;
          snk = nullptr;
          stop = false;
          waiting_for_final_report = true;
        } else {
          VAST_WARN("{} received DOWN from inexplicable actor: {}",
                    inv.full_name, msg.reason);
        }
        if (msg.reason && msg.reason != caf::exit_reason::user_shutdown) {
          VAST_WARN("{} received error message: {}", inv.full_name, msg.reason);
          err = std::move(msg.reason);
        }
      },
      [&]([[maybe_unused]] const performance_report& report) {
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
        // Log a set of named measurements.
        for (const auto& [name, measurement] : report.data) {
          if (auto rate = measurement.rate_per_sec(); std::isfinite(rate))
            VAST_INFO("{} processed {} events at a rate of {} events/sec in {}",
                      name, measurement.events, static_cast<uint64_t>(rate),
                      to_string(measurement.duration));
          else
            VAST_INFO("{} processed {} events", name, measurement.events);
        }
#endif
      },
      [&]([[maybe_unused]] const std::string& name,
          [[maybe_unused]] const query_status& query) {
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
        if (auto rate
            = measurement{query.runtime, query.processed}.rate_per_sec();
            std::isfinite(rate))
          VAST_INFO("{} processed {} candidates at a rate of {} candidates/sec "
                    "and shipped {} results in {}",
                    name, query.processed, static_cast<uint64_t>(rate),
                    query.shipped, to_string(query.runtime));
        else
          VAST_INFO("{} processed {} candidates and shipped {} results in {}",
                    name, query.processed, query.shipped,
                    to_string(query.runtime));
#endif
        if (waiting_for_final_report)
          stop = true;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} got {}", inv.full_name, ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM) {
          self->send_exit(exporter, caf::exit_reason::user_shutdown);
          self->send_exit(snk, caf::exit_reason::user_shutdown);
        }
      })
    .until([&] {
      return stop;
    });
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
