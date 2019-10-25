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
#include "vast/format/pcap.hpp"
#include "vast/format/zeek.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/read_query.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/sink.hpp"
#include "vast/system/sink_command.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/tracker.hpp"

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

caf::message
pivot_command(const command::invocation& invocation, caf::actor_system& sys) {
  VAST_TRACE(invocation);
  // Read query from input file, STDIN or CLI arguments.
  auto query = read_query(invocation, "export.read", size_t{1});
  if (!query)
    return caf::make_message(std::move(query.error()));
  auto& options = invocation.options;
  auto& target = invocation.arguments[0];
  // using caf::get_or;
  auto limit
    = get_or(options, "export.max-events", defaults::export_::max_events);
  caf::actor writer;
  if (detail::starts_with(target, "pcap")) {
    using defaults_t = defaults::export_::pcap;
    std::string category = defaults_t::category;
    auto output = get_or(options, category + ".write", defaults_t::write);
    auto flush = get_or(options, category + ".flush-interval",
                        defaults_t::flush_interval);
    format::pcap::writer w{output, flush};
    writer = sys.spawn(sink<format::pcap::writer>, std::move(w), limit);
  } else if (detail::starts_with(target, "suricata")) {
    auto w = make_writer<format::json::writer>(sys, invocation.options);
    if (!w)
      return make_message(w.error());
    writer = *w;
  } else if (detail::starts_with(target, "zeek")) {
    auto w = make_writer<format::zeek::writer>(sys, invocation.options);
    if (!w)
      return make_message(w.error());
    writer = *w;
  } else
    return make_message(make_error(ec::unimplemented, "pivoting is only "
                                                      "implemented for pcap, "
                                                      "suricata and zeek"));
  caf::scoped_actor self{sys};
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, invocation.options, content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
                 ? caf::get<caf::actor>(node_opt)
                 : caf::get<scope_linked_actor>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  // TODO(ch9412): Factor guard into a function.
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = signal_monitor::run_guarded(sig_mon_thread, sys, 750ms, self);
  // Spawn exporter at the node.
  caf::actor exp;
  auto exporter_invocation
    = command::invocation{invocation.options, "spawn exporter", {*query}};
  VAST_DEBUG(invocation.full_name,
             "spawns exporter with parameters:", exporter_invocation);
  error err;
  self->request(node, caf::infinite, std::move(exporter_invocation))
    .receive(
      [&](caf::actor& a) {
        exp = std::move(a);
        if (!exp)
          err = make_error(ec::invalid_result, "remote spawn returned nullptr");
      },
      [&](error& e) { err = std::move(e); });
  if (err) {
    self->send_exit(writer, caf::exit_reason::user_shutdown);
    return caf::make_message(std::move(err));
  }
  // Spawn pivoter at the node.
  caf::actor piv;
  auto pivoter_invocation = command::invocation{
    invocation.options, "spawn pivoter", {invocation.arguments[0], *query}};
  VAST_DEBUG(invocation.full_name,
             "spawns exporter with parameters:", pivoter_invocation);
  self->request(node, caf::infinite, std::move(pivoter_invocation))
    .receive(
      [&](caf::actor& a) {
        piv = std::move(a);
        if (!piv)
          err = make_error(ec::invalid_result, "remote spawn returned nullptr");
      },
      [&](error& e) { err = std::move(e); });
  if (err) {
    self->send_exit(exp, caf::exit_reason::user_shutdown);
    self->send_exit(writer, caf::exit_reason::user_shutdown);
    return caf::make_message(std::move(err));
  }
  self->request(node, caf::infinite, get_atom::value)
    .receive(
      [&](const std::string& id, system::registry& reg) {
        // Assign accountant to sink.
        VAST_DEBUG(invocation.full_name, "assigns accountant from node", id,
                   "to new sink");
        auto er = reg.components[id].find("accountant");
        if (er != reg.components[id].end()) {
          auto accountant = er->second.actor;
          self->send(writer, caf::actor_cast<accountant_type>(accountant));
        }
      },
      [&](error& e) { err = std::move(e); });
  if (err) {
    self->send_exit(writer, caf::exit_reason::user_shutdown);
    return caf::make_message(std::move(err));
  }
  self->monitor(writer);
  self->monitor(piv);
  // Start the exporter.
  self->send(exp, system::sink_atom::value, piv);
  // (Ab)use query_statistics as done message.
  self->send(exp, system::statistics_atom::value, piv);
  self->send(piv, system::sink_atom::value, writer);
  self->send(exp, system::run_atom::value);
  auto stop = false;
  self
    ->do_receive(
      [&](caf::down_msg& msg) {
        if (msg.source == node) {
          VAST_DEBUG_ANON(__func__, "received DOWN from node");
          self->send_exit(writer, caf::exit_reason::user_shutdown);
          self->send_exit(exp, caf::exit_reason::user_shutdown);
          self->send_exit(piv, caf::exit_reason::user_shutdown);
        } else if (msg.source == piv) {
          VAST_DEBUG(invocation.full_name, "received DOWN from pivoter");
          self->send_exit(exp, caf::exit_reason::user_shutdown);
          self->send_exit(writer, caf::exit_reason::user_shutdown);
        } else if (msg.source == writer) {
          VAST_DEBUG(invocation.full_name, "received DOWN from sink");
          self->send_exit(exp, caf::exit_reason::user_shutdown);
          self->send_exit(piv, caf::exit_reason::user_shutdown);
        } else {
          VAST_ASSERT(!"received DOWN from inexplicable actor");
        }
        if (msg.reason) {
          VAST_WARNING(invocation.full_name, "received error message:",
                       self->system().render(msg.reason));
          err = std::move(msg.reason);
        }
        stop = true;
      },
      [&](system::signal_atom, int signal) {
        VAST_DEBUG(invocation.full_name, "got " << ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM) {
          self->send_exit(exp, caf::exit_reason::user_shutdown);
          self->send_exit(piv, caf::exit_reason::user_shutdown);
          self->send_exit(writer, caf::exit_reason::user_shutdown);
        }
      })
    .until([&] { return stop; });
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
