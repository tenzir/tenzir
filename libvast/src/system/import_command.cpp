//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/import_command.hpp"

#include "vast/command.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/actors.hpp"
#include "vast/system/make_pipelines.hpp"
#include "vast/system/make_source.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/parse_query.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/uuid.hpp"

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
  VAST_DEBUG("{} received node handle", inv.full_name);
  // Get node components.
  auto components = get_node_components< //
    accountant_actor, catalog_actor, importer_actor>(self, node);
  if (!components)
    return caf::make_message(std::move(components.error()));
  auto& [accountant, catalog, importer] = *components;
  if (!catalog)
    return caf::make_message(caf::make_error( //
      ec::missing_component, "catalog"));
  if (!importer)
    return caf::make_message(caf::make_error( //
      ec::missing_component, "importer"));
  auto pipelines
    = make_pipelines(pipelines_location::client_source, inv.options);
  if (!pipelines)
    return caf::make_message(pipelines.error());
  if (!inv.arguments.empty()) {
    auto parse_result = parse_query(inv.arguments);
    if (!parse_result) {
      return caf::make_message(parse_result.error());
    }
    auto [expr, pipeline] = std::move(*parse_result);
    if (pipeline) {
      pipelines->push_back(std::move(*pipeline));
    }
  }
  const auto format = std::string{inv.name()};
  // Start the source.
  auto src_result = make_source(sys, format, inv, accountant, catalog, importer,
                                std::move(*pipelines));
  if (!src_result)
    return caf::make_message(std::move(src_result.error()));
  auto src = std::move(*src_result);
  bool stop = false;
  caf::error err;
  self->request(node, caf::infinite, atom::put_v, src, "source")
    .receive(
      [&](atom::ok) {
        VAST_DEBUG("{} registered source at node", __PRETTY_FUNCTION__);
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
          VAST_DEBUG("{} received DOWN from node importer",
                     __PRETTY_FUNCTION__);
          self->send_exit(src, caf::exit_reason::user_shutdown);
          err = ec::remote_node_down;
          stop = true;
        } else if (msg.source == src) {
          VAST_DEBUG("{} received DOWN from source", __PRETTY_FUNCTION__);
          // Wait for the ingest to complete. This must also be done when
          // the index is in the same process because otherwise it can
          // happen that the index gets an exit message before the first
          // table slice arrives on the stream.
          if (caf::get_or(inv.options, "vast.import.blocking", false)
              || caf::get_or(inv.options, "vast.node", false))
            self->send(importer, atom::subscribe_v, atom::flush_v,
                       caf::actor_cast<flush_listener_actor>(self));
          else
            stop = true;
        } else {
          VAST_DEBUG("{} received unexpected DOWN from {}", __PRETTY_FUNCTION__,
                     msg.source);
          VAST_ASSERT(!"unexpected DOWN message");
        }
      },
      [&](atom::flush) {
        VAST_DEBUG("{} received flush from IMPORTER", __PRETTY_FUNCTION__);
        stop = true;
      },
      [&](atom::signal, int signal) {
        VAST_DEBUG("{} received signal {}", __PRETTY_FUNCTION__,
                   ::strsignal(signal));
        VAST_ASSERT(signal == SIGINT || signal == SIGTERM);
        self->send_exit(src, caf::exit_reason::user_shutdown);
      })
    .until(stop);
  if (err)
    return caf::make_message(std::move(err));
  // The flush listener based blocking mechanism is flawed and fails quite
  // often. As a workaround we force a flush-to-disk of all data that is
  // currently held in memory.
  if (caf::get_or(inv.options, "vast.import.blocking", false)) {
    auto components
      = system::get_node_components<system::index_actor>(self, node);
    if (!components)
      return caf::make_message(std::move(components.error()));
    auto [index] = std::move(*components);
    // Flush!
    auto result = caf::message{};
    self->request(index, caf::infinite, atom::flush_v)
      .receive(
        []() {
          // nop
        },
        [&](caf::error& err) {
          result = caf::make_message(std::move(err));
        });
    return result;
  }
  return {};
}

} // namespace vast::system
