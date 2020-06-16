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
#include "vast/detail/make_io_stream.hpp"
#include "vast/error.hpp"
#include "vast/format/csv.hpp"
#include "vast/format/json.hpp"
#include "vast/format/zeek.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/read_query.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/sink_command.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/tracker.hpp"

#if VAST_HAVE_PCAP
#  include "vast/format/pcap.hpp"
#endif

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include <csignal>
#include <string>
#include <string_view>

using namespace std::chrono_literals;

namespace vast::system {

namespace {

template <class Writer>
caf::expected<caf::actor>
make_writer(caf::actor_system& sys, const caf::settings& options) {
  using defaults = typename Writer::defaults;
  using ostream_ptr = std::unique_ptr<std::ostream>;
  std::string category = defaults::category;
  if constexpr (std::is_constructible_v<Writer, ostream_ptr>) {
    auto output = get_or(options, category + ".write", defaults::write);
    auto uds = get_or(options, category + ".uds", false);
    auto out = detail::make_output_stream(output, uds);
    if (!out)
      return out.error();
    Writer writer{std::move(*out)};
    return sys.spawn(sink<Writer>, std::move(writer), max_events);
  } else {
    Writer writer;
    return sys.spawn(sink<Writer>, std::move(writer), max_events);
  }
}

} // namespace

caf::message pivot_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE(inv);
  // Read query from input file, STDIN or CLI arguments.
  auto query = read_query(inv, "export.read", size_t{1});
  if (!query)
    return caf::make_message(std::move(query.error()));
  auto& options = inv.options;
  auto& target = inv.arguments[0];
  // using caf::get_or;
  auto limit
    = get_or(options, "export.max-events", defaults::export_::max_events);
  caf::actor writer;
  if (detail::starts_with(target, "pcap")) {
#if VAST_HAVE_PCAP
    using defaults_t = defaults::export_::pcap;
    std::string category = defaults_t::category;
    auto output = get_or(options, category + ".write", defaults_t::write);
    auto flush = get_or(options, category + ".flush-interval",
                        defaults_t::flush_interval);
    format::pcap::writer w{output, flush};
    writer = sys.spawn(sink<format::pcap::writer>, std::move(w), limit);
#else
    return make_message(make_error(ec::invalid_configuration, "pcap support "
                                                              "unavailable"));
#endif
  } else if (detail::starts_with(target, "suricata")) {
    auto w = make_writer<format::json::writer>(sys, inv.options);
    if (!w)
      return make_message(w.error());
    writer = *w;
  } else if (detail::starts_with(target, "zeek")) {
    auto w = make_writer<format::zeek::writer>(sys, inv.options);
    if (!w)
      return make_message(w.error());
    writer = *w;
  } else
    return make_message(make_error(ec::unimplemented, "pivoting is only "
                                                      "implemented for pcap, "
                                                      "suricata and zeek"));
  caf::scoped_actor self{sys};
  auto writer_guard = caf::detail::make_scope_guard(
    [&] { self->send_exit(writer, caf::exit_reason::user_shutdown); });
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
  VAST_DEBUG(&invocation, "spawns exporter with parameters:", spawn_exporter);
  auto exp = spawn_at_node(self, node, spawn_exporter);
  if (!exp)
    return caf::make_message(std::move(exp.error()));
  auto exp_guard = caf::detail::make_scope_guard(
    [&] { self->send_exit(*exp, caf::exit_reason::user_shutdown); });
  // Spawn pivoter at the node.
  auto spawn_pivoter
    = invocation{inv.options, "spawn pivoter", {inv.arguments[0], *query}};
  VAST_DEBUG(&invocation, "spawns pivoter with parameters:", spawn_pivoter);
  auto piv = spawn_at_node(self, node, spawn_pivoter);
  if (!piv)
    return caf::make_message(std::move(piv.error()));
  auto piv_guard = caf::detail::make_scope_guard(
    [&] { self->send_exit(*piv, caf::exit_reason::user_shutdown); });
  // Register the accountant at the Sink.
  auto components = get_node_components(self, node, {"accountant"});
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto& [accountant] = *components;
  if (accountant) {
    VAST_DEBUG(invocation.full_name, "assigns accountant to writer");
    self->send(writer, caf::actor_cast<accountant_type>(accountant));
  }
  caf::error err;
  self->monitor(writer);
  self->monitor(*piv);
  // Start the exporter.
  self->send(*exp, atom::sink_v, *piv);
  // (Ab)use query_statistics as done message.
  self->send(*exp, atom::statistics_v, *piv);
  self->send(*piv, atom::sink_v, writer);
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
        } else if (msg.source == writer) {
          VAST_DEBUG(inv.full_name, "received DOWN from sink");
          writer_guard.disable();
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
        VAST_DEBUG(inv.full_name, "got " << ::strsignal(signal));
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
