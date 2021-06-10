//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/import_command.hpp"

#include "vast/command.hpp"
#include "vast/defaults.hpp"
#include "vast/error.hpp"
#include "vast/format/reader.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/make_source.hpp"
#include "vast/system/make_transforms.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/transformer.hpp"

#include <caf/make_message.hpp>
#include <caf/settings.hpp>

#include <csignal>
#include <string>
#include <utility>

namespace vast::system {

caf::message import_command(const invocation& inv, caf::actor_system& sys) {
  VAST_TRACE_SCOPE("{}", inv);
  auto self = caf::scoped_actor{sys};
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, inv.options, content(sys.config()));
  if (auto* err = std::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  const auto& node = std::holds_alternative<node_actor>(node_opt)
                       ? std::get<node_actor>(node_opt)
                       : std::get<scope_linked<node_actor>>(node_opt).get();
  VAST_DEBUG("{} got node", inv.full_name);
  // Get node components.
  auto components = get_node_components< //
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
  auto transforms
    = make_transforms(transforms_location::client_source, inv.options);
  if (!transforms)
    return caf::make_message(transforms.error());
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  const auto format = std::string{inv.name()};
  // Set up the flush listener if needed.
  const auto blocking = caf::get_or(inv.options, "vast.import.blocking", false)
                        && !caf::get_or(inv.options, "vast.node", false);
  auto flush_listener = std::optional<flush_listener_actor>{};
  if (blocking)
    flush_listener = caf::actor_cast<flush_listener_actor>(self);
  int expected_flushes = 0;
  int total_flushes = 0;
  // Start the source.
  auto src_result
    = make_source(sys, format, inv, accountant, type_registry, importer,
                  flush_listener, std::move(*transforms));
  if (!src_result)
    return caf::make_message(std::move(src_result.error()));
  auto src = std::move(*src_result);
  bool stop = false;
  caf::error err;
  self->request(node, caf::infinite, atom::put_v, src, "source")
    .receive(
      [&](atom::ok) {
        VAST_DEBUG("{} registered source at node", inv.full_name);
      },
      [&](caf::error error) {
        err = std::move(error);
      });
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
          VAST_DEBUG("{} received DOWN from node importer", inv.full_name);
          self->send_exit(src, caf::exit_reason::user_shutdown);
          err = ec::remote_node_down;
          stop = true;
        } else if (msg.source == src) {
          VAST_DEBUG("{} received DOWN from source", inv.full_name);
          if (blocking)
            VAST_INFO("{} waits until all items events are processed",
                      inv.full_name);
          else
            stop = true;
        } else {
          VAST_DEBUG("{} received unexpected DOWN from {}", inv.full_name,
                     msg.source);
          VAST_ASSERT(!"unexpected DOWN message");
        }
      },
      [&](atom::flush, atom::add) {
        ++expected_flushes;
        VAST_ASSERT(expected_flushes != 0);
      },
      [&](atom::flush, atom::sub) {
        static constexpr int minimum_flushes = 2;
        --expected_flushes;
        ++total_flushes;
        VAST_WARN("{} received {}/{} minimum flushes", inv.full_name,
                  total_flushes, minimum_flushes + expected_flushes);
        if (expected_flushes == 0 && total_flushes >= minimum_flushes)
          stop = true;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} received signal {}", inv.full_name, ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM)
          self->send_exit(src, caf::exit_reason::user_shutdown);
      })
    .until(stop);
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
