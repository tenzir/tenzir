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

#include "vast/command.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/fwd.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/make_source.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/type_registry.hpp"

#include <csignal>
#include <string>
#include <utility>

namespace vast::system {

template <class Reader, class Defaults>
caf::message import_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE(inv.full_name, VAST_ARG("options", inv.options), VAST_ARG(sys));
  auto self = caf::scoped_actor{sys};
  // Placeholder thingies.
  auto err = caf::error{};
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
                 ? caf::get<caf::actor>(node_opt)
                 : caf::get<scope_linked_actor>(node_opt).get();
  VAST_DEBUG(inv.full_name, "got node");
  // Get node components.
  auto components = get_node_components(
    self, node, {"accountant", "type-registry", "importer"});
  if (!components)
    return make_message(std::move(components.error()));
  auto& [accountant, type_registry, importer] = *components;
  if (!type_registry)
    return make_message(make_error(ec::missing_component, "type-registry"));
  if (!importer)
    return make_message(make_error(ec::missing_component, "importer"));
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  // Start the source.
  auto src_result = make_source<Reader, Defaults>(
    self, sys, inv, caf::actor_cast<accountant_type>(accountant),
    caf::actor_cast<type_registry_type>(type_registry), importer);
  if (!src_result)
    return make_message(std::move(src_result.error()));
  auto src = std::move(src_result->src);
  auto name = std::move(src_result->name);
  bool stop = false;
  self->request(node, caf::infinite, atom::put_v, src, "source")
    .receive([&](atom::ok) { VAST_DEBUG(name, "registered source at node"); },
             [&](caf::error error) { err = std::move(error); });
  self->monitor(src);
  self->monitor(importer);
  self
    ->do_receive(
      [&, importer = importer](const caf::down_msg& msg) {
        if (msg.source == importer) {
          VAST_DEBUG(name, "received DOWN from node importer");
          self->send_exit(src, caf::exit_reason::user_shutdown);
          err = ec::remote_node_down;
          stop = true;
        } else if (msg.source == src) {
          VAST_DEBUG(name, "received DOWN from source");
          if (caf::get_or(inv.options, "import.blocking", false))
            self->send(importer, atom::subscribe_v, atom::flush::value, self);
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
    return make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
