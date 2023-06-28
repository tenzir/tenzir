//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sink_command.hpp"

#include "vast/actors.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/node_control.hpp"
#include "vast/query_status.hpp"
#include "vast/read_query.hpp"
#include "vast/report.hpp"
#include "vast/scope_linked.hpp"
#include "vast/spawn_or_connect_to_node.hpp"

#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include <csignal>
#include <string>

using namespace std::chrono_literals;

namespace vast {

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
  auto query = read_query(inv, "tenzir.export.read", must_provide_query::no);
  if (!query)
    return caf::make_message(std::move(query.error()));
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node = std::holds_alternative<node_actor>(node_opt)
                       ? std::get<node_actor>(node_opt)
                       : std::get<scope_linked<node_actor>>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  auto spawn_exporter = invocation{inv.options, "spawn exporter", {*query}};
  VAST_DEBUG("{} spawns exporter with parameters: {}", inv, spawn_exporter);
  auto maybe_exporter = spawn_at_node(self, node, spawn_exporter);
  if (!maybe_exporter)
    return caf::make_message(std::move(maybe_exporter.error()));
  exporter = caf::actor_cast<exporter_actor>(std::move(*maybe_exporter));
  caf::error err = caf::none;
  self->request(exporter, caf::infinite, atom::sink_v, snk)
    .receive(
      [&]() {
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
                                                  "tenzir.export.timeout")) {
    if (auto timeout = to<duration>(*timeout_str))
      self->delayed_send(self, *timeout, atom::shutdown_v, *timeout);
    else
      VAST_ERROR("{} was unable parse timeout option {} as duration: {}",
                 inv.full_name, *timeout_str, timeout.error());
  }
  // Start the receive-loop.
  auto stop = false;
  self
    ->do_receive(
      [&](atom::shutdown, const duration& timeout) {
        VAST_INFO("{} shuts down after {} timeout", inv.full_name,
                  to_string(timeout));
        stop = true;
        err = caf::make_error(ec::timeout,
                              fmt::format("{} shut down after {} timeout",
                                          inv.full_name, to_string(timeout)));
      },
      [&, node_addr = node->address(), snk_addr = snk->address(),
       exporter_addr = exporter->address()](caf::down_msg& msg) {
        stop = true;
        VAST_DEBUG("{} received DOWN from sink {}", inv.full_name, msg.reason);
        self->send_exit(exporter, caf::exit_reason::user_shutdown);
        exporter = nullptr;
        snk = nullptr;
        if (msg.reason && msg.reason != caf::exit_reason::user_shutdown) {
          err = std::move(msg.reason);
        }
      },
      [&]([[maybe_unused]] const performance_report& report) {
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
        // Log a set of named measurements.
        for (const auto& [name, measurement, _] : report.data) {
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
          [[maybe_unused]] const query_status& query_status) {
#if VAST_LOG_LEVEL >= VAST_LOG_LEVEL_INFO
        if (auto rate
            = measurement{query_status.runtime, query_status.processed}
                .rate_per_sec();
            std::isfinite(rate))
          VAST_INFO("{} processed {} candidates at a rate of {} candidates/sec "
                    "and shipped {} results in {}",
                    name, query_status.processed, static_cast<uint64_t>(rate),
                    query_status.shipped, to_string(query_status.runtime));
        else
          VAST_INFO("{} processed {} candidates and shipped {} results in {}",
                    name, query_status.processed, query_status.shipped,
                    to_string(query_status.runtime));
#endif
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} got {}", inv.full_name, ::strsignal(signal));
        VAST_ASSERT(signal == SIGINT || signal == SIGTERM);
        stop = true;
      })
    .until([&] {
      return stop;
    });
  if (err)
    return caf::make_message(std::move(err));
  return {};
}

} // namespace vast
