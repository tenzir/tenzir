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

#include "vast/system/start_command.hpp"

#include <csignal>
#include <iostream>
#include <thread>

#include "vast/config.hpp"

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#ifdef VAST_USE_OPENSSL
#include <caf/openssl/all.hpp>
#endif // VAST_USE_OPENSSL

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/endpoint.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/eraser.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_node.hpp"
#include "vast/system/tracker.hpp"

namespace vast::system {

using namespace std::chrono_literals;

caf::message start_command(const command&, caf::actor_system& sys,
                           caf::settings& options,
                           command::argument_iterator begin,
                           command::argument_iterator end) {
  VAST_UNUSED(begin, end);
  VAST_TRACE(VAST_ARG(options), VAST_ARG("args", begin, end));
  // Fetch SSL settings from config.
  auto& sys_cfg = sys.config();
  auto use_encryption = !sys_cfg.openssl_certificate.empty()
                        || !sys_cfg.openssl_key.empty()
                        || !sys_cfg.openssl_passphrase.empty()
                        || !sys_cfg.openssl_capath.empty()
                        || !sys_cfg.openssl_cafile.empty();
  // Construct an endpoint.
  auto node_endpoint = make_default_endpoint();
  if (auto str = caf::get_if<std::string>(&options, "endpoint"))
    if (!parsers::endpoint(*str, node_endpoint))
      make_error(ec::parse_error, "invalid endpoint", *str);
  // Get a convenient and blocking way to interact with actors.
  caf::scoped_actor self{sys};
  // Spawn our node.
  auto node_opt = spawn_node(self, content(sys.config()));
  if (!node_opt)
    return caf::make_message(std::move(node_opt.error()));
  auto& node = node_opt->get();
  // Publish our node.
  auto host = node_endpoint.host.empty() ? nullptr : node_endpoint.host.c_str();
  auto publish = [&]() -> caf::expected<uint16_t> {
    if (use_encryption)
#ifdef VAST_USE_OPENSSL
      return caf::openssl::publish(node, node_endpoint.port, host);
#else
      return make_error(ec::unspecified, "not compiled with OpenSSL support");
#endif
    auto& mm = sys.middleman();
    auto reuse_address = true;
    return mm.publish(node, node_endpoint.port, host, reuse_address);
  };
  auto bound_port = publish();
  if (!bound_port)
    return caf::make_message(std::move(bound_port.error()));
  VAST_INFO_ANON("VAST node is listening on", (host ? host : "")
                 << ':' << *bound_port);
  // Start ERASER if we have an aging query.
  caf::actor eraser;
  auto eraser_query = caf::get_or(options, "aging-query", "");
  if (!eraser_query.empty()) {
    // Parse the query here once to verify that we have a valid expression
    // before spawning the ERASER.
    if (auto expr = to<expression>(eraser_query); !expr) {
      VAST_WARNING(__func__, "got an invalid ERASER query:" << eraser_query);
    } else {
      VAST_DEBUG(__func__, "spawns an ERASER for query:" << eraser_query);
      // Fetch ARCHIVE and INDEX from NODE.
      caf::actor index;
      caf::actor archive;
      self->request(node, caf::infinite, get_atom::value)
        .receive(
          [&](const std::string& id, registry& reg) {
            // Read INDEX from registry.
            auto by_name = [&](const char* actor_name) -> caf::actor {
              auto [first, last] = reg.components[id].equal_range(actor_name);
              if (first != last)
                return first->second.actor;
              return nullptr;
            };
            index = by_name("index");
            archive = by_name("archive");
          },
          [&](error& e) {
            VAST_WARNING(__func__,
                         "received an error while accessing the NODE:",
                         sys.render(e));
          });
      if (index != nullptr && archive != nullptr) {
        eraser = sys.spawn(system::eraser,
                           caf::get_or(options, "aging-frequency",
                                       defaults::system::aging_frequency),
                           std::move(eraser_query), std::move(index),
                           std::move(archive));
      } else {
        VAST_WARNING(__func__, "failed to get INDEX and ARCHIVE from NODE");
      }
    }
  }
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = signal_monitor::run_guarded(sig_mon_thread, sys, 750ms, self);
  // Run main loop.
  caf::error err;
  auto stop = false;
  self->monitor(node);
  self
    ->do_receive(
      [&](caf::down_msg& msg) {
        VAST_ASSERT(msg.source == node);
        VAST_DEBUG_ANON("... received DOWN from node");
        stop = true;
        if (msg.reason != caf::exit_reason::user_shutdown)
          err = std::move(msg.reason);
      },
      [&](signal_atom, int signal) {
        VAST_DEBUG_ANON("... got " << ::strsignal(signal));
        if (signal == SIGINT || signal == SIGTERM)
          self->send_exit(node, caf::exit_reason::user_shutdown);
        else
          self->send(node, signal_atom::value, signal);
      })
    .until([&] { return stop; });
  self->send_exit(eraser, caf::exit_reason::user_shutdown);
  return caf::make_message(std::move(err));
}

} // namespace vast::system
