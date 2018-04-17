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

#include <iostream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#ifdef VAST_USE_OPENSSL
#include <caf/openssl/all.hpp>
#endif // VAST_USE_OPENSSL

#include "vast/logger.hpp"

#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn.hpp"

#include "vast/concept/parseable/vast/endpoint.hpp"

using namespace caf;

namespace vast::system {
using namespace std::chrono_literals;

start_command::start_command(command* parent, std::string_view name)
  : node_command{parent, name},
    spawn_bare_node{false},
    in_foreground{false} {
  add_opt("bare", "spawn empty node without any components", spawn_bare_node);
  add_opt("foreground", "run in foreground (do not daemonize)", in_foreground);
}

int start_command::run_impl(actor_system& sys, option_map& options,
                        const_iterator, const_iterator) {
  CAF_LOG_TRACE(CAF_ARG(options));
  // Fetch SSL settings from config.
  auto& sys_cfg = sys.config();
  auto use_encryption = !sys_cfg.openssl_certificate.empty()
                        || !sys_cfg.openssl_key.empty()
                        || !sys_cfg.openssl_passphrase.empty()
                        || !sys_cfg.openssl_capath.empty()
                        || !sys_cfg.openssl_cafile.empty();
  // Fetch endpoint from config.
  auto endpoint_opt = get<std::string>(options, "endpoint");
  if (!endpoint_opt) {
    VAST_ERROR("endpoint missing in options map");
    return EXIT_FAILURE;
  }
  endpoint node_endpoint;
  if (!parsers::endpoint(*endpoint_opt, node_endpoint)) {
    VAST_ERROR("invalid endpoint:", *endpoint_opt);
    return EXIT_FAILURE;
  }
  // Get a convenient and blocking way to interact with actors.
  scoped_actor self{sys};
  // Spawn our node.
  auto node_opt = spawn_node(self, options);
  if (!node_opt) {
    std::cerr << sys.render(node_opt.error()) << std::endl;
    return EXIT_FAILURE;
  }
  auto node = std::move(*node_opt);
  // Publish our node.
  auto host = node_endpoint.host.empty() ? nullptr : node_endpoint.host.c_str();
  auto publish = [&]() -> expected<uint16_t> {
    if (use_encryption)
#ifdef VAST_USE_OPENSSL
      return openssl::publish(node, node_endpoint.port, host);
#else
      return make_error(ec::unspecified, "not compiled with OpenSSL support");
#endif
    auto& mm = sys.middleman();
    return mm.publish(node, node_endpoint.port, host);
  };
  auto bound_port = publish();
  if (!bound_port) {
    VAST_ERROR(self->system().render(bound_port.error()));
    self->send_exit(node, exit_reason::user_shutdown);
    return EXIT_FAILURE;
  }
  VAST_INFO("listening on", (host ? host : "") << ':' << *bound_port);
  // Spawn signal handler.
  auto sig_mon = self->spawn<detached>(system::signal_monitor, 750ms, self);
  auto guard = caf::detail::make_scope_guard([&] {
    self->send_exit(sig_mon, exit_reason::user_shutdown);
  });
  // Run main loop.
  auto rc = 0;
  auto stop = false;
  self->monitor(node);
  self->do_receive(
    [&](const down_msg& msg) {
      VAST_ASSERT(msg.source == node);
      VAST_DEBUG("received DOWN from node");
      stop = true;
      if (msg.reason != exit_reason::user_shutdown)
        rc = 1;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG("got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM)
        self->send_exit(node, exit_reason::user_shutdown);
      else
        self->send(node, system::signal_atom::value, signal);
    }
  ).until([&] { return stop; });
  return rc;
}

} // namespace vast::system
