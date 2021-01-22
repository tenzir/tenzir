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

#pragma once

#include "vast/fwd.hpp"

#include "vast/command.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/make_source.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"

#include <caf/make_message.hpp>

#include <csignal>
#include <string>
#include <utility>

namespace vast::system {

template <class Reader, class Defaults>
caf::message import_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE(inv.full_name, VAST_ARG("options", inv.options), VAST_ARG(sys));
  auto self = caf::scoped_actor{sys};
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
                 ? caf::get<caf::actor>(node_opt)
                 : caf::get<scope_linked_actor>(node_opt).get();
  VAST_DEBUG(inv.full_name, "got node");
  // Get node components.
  auto components = get_typed_node_components< //
    accountant_actor, type_registry_actor, importer_actor>(self, node);
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto& [accountant, type_registry, importer] = *components;
  if (!type_registry)
    return caf::make_message(caf::make_error( //
      ec::missing_component, "type-registry"));
  if (!importer)
    return caf::make_message(caf::make_error( //
      ec::missing_component, "importer"));
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  // Start the source.
  auto src_result = make_source<Reader, Defaults>(self, sys, inv, accountant,
                                                  type_registry, importer);
  if (!src_result)
    return caf::make_message(std::move(src_result.error()));
  auto src = std::move(src_result->src);
  auto name = std::move(src_result->name);
  bool stop = false;
  caf::error err;
  self->request(node, caf::infinite, atom::put_v, src, "source")
    .receive([&](atom::ok) { VAST_DEBUG(name, "registered source at node"); },
             [&](caf::error error) { err = std::move(error); });
  if (err) {
    self->send_exit(src, caf::exit_reason::user_shutdown);
    return caf::make_message(std::move(err));
  }
  self->monitor(src);
  self->monitor(importer);
  self
    ->do_receive(
      // C++20: remove explicit 'importer' parameter passing.
      [&, importer = importer](const caf::down_msg& msg) {
        if (msg.source == importer) {
          VAST_DEBUG(name, "received DOWN from node importer");
          self->send_exit(src, caf::exit_reason::user_shutdown);
          err = ec::remote_node_down;
          stop = true;
        } else if (msg.source == src) {
          VAST_DEBUG(name, "received DOWN from source");
          if (caf::get_or(inv.options, "vast.import.blocking", false))
            self->send(importer, atom::subscribe_v, atom::flush::value,
                       caf::actor_cast<flush_listener_actor>(self));
          else
            stop = true;
        } else {
          VAST_DEBUG(name, "received unexpected DOWN from", msg.source);
          VAST_ASSERT(!"unexpected DOWN message");
        }
      },
      [&](atom::flush) {
        VAST_DEBUG(name, "received flush from IMPORTER");
        stop = true;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG(name, "received signal", ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM)
          self->send_exit(src, caf::exit_reason::user_shutdown);
      })
    .until(stop);
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

template <class Reader, class SimdjsonReader, class Defaults>
caf::message
import_command_json(const invocation& inv, caf::actor_system& sys) {
  const auto use_simdjson = caf::get_or(
    inv.options, Defaults::category + std::string{".simdjson"}, false);

  if (use_simdjson)
    return import_command<SimdjsonReader, Defaults>(inv, sys);
  return import_command<Reader, Defaults>(inv, sys);
}

template <template <class S, class B> class Reader,
          template <class S, class B> class SimdjsonReader, typename Selector,
          class Defaults>
caf::message import_command_json_with_benchmark(const invocation& inv,
                                                caf::actor_system& sys) {
  const auto bench_value = caf::get_if<std::string>(
    &inv.options, Defaults::category + std::string{".benchmark"});
  if (bench_value) {
    if (*bench_value == "cycleclock")
      return import_command_json<
        Reader<Selector, format::bench::cycleclock_benchmark_mixin<4>>,
        SimdjsonReader<Selector, format::bench::cycleclock_benchmark_mixin<4>>,
        Defaults>(inv, sys);
    else if (*bench_value == "timespec")
      return import_command_json<
        Reader<Selector, format::bench::timespec_benchmark_mixin<4>>,
        SimdjsonReader<Selector, format::bench::timespec_benchmark_mixin<4>>,
        Defaults>(inv, sys);

    return caf::make_message(
      make_error(ec::invalid_configuration, "unknown benchmark value"));
  }

  return import_command_json<
    Reader<Selector, format::bench::noop_benchmark_mixin>,
    SimdjsonReader<Selector, format::bench::noop_benchmark_mixin>, Defaults>(
    inv, sys);
}

} // namespace vast::system
