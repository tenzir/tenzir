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

#include "vast/system/run_export.hpp"

#include <iostream>

#include <caf/all.hpp>
#include <caf/io/all.hpp>
#ifdef VAST_USE_OPENSSL
#include <caf/openssl/all.hpp>
#endif // VAST_USE_OPENSSL

#include "vast/logger.hpp"

#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn.hpp"

using namespace caf;

namespace vast::system {
using namespace std::chrono_literals;

run_export::run_export(command* parent, std::string_view name)
  : base_command(parent, name) {
  // nop
}

int run_export::run_impl(actor_system& sys, option_map& options,
                         caf::message args) {
  // Get a convenient and blocking way to interact with actors.
  scoped_actor self{sys};
  // Get VAST node.
  auto node_opt = spawn_or_connect_to_node(self, options);
  if (!node_opt)
    return EXIT_FAILURE;
  auto node = std::move(*node_opt);
  /// Spawn an actor that takes care of CTRL+C and friends.
  auto sig_mon = self->spawn<detached>(system::signal_monitor, 750ms, self);
  auto guard = caf::detail::make_scope_guard([&] {
    self->send_exit(sig_mon, exit_reason::user_shutdown);
  });
  // Spawn a sink.
  auto opts = system::options{args, {}, {}};
  VAST_DEBUG("spawning sink with parameters:", deep_to_string(opts.params));
  auto snk = system::spawn_sink(actor_cast<local_actor*>(self), opts);
  if (!snk) {
    VAST_ERROR("failed to spawn sink:", self->system().render(snk.error()));
    return 1;
  }
  // Spawn exporter at the node.
  actor exp;
  args = make_message("exporter") + opts.params;
  VAST_DEBUG("spawning exporter with parameters:", to_string(args));
  self->request(node, infinite, "spawn", args).receive(
    [&](const actor& a) {
      exp = a;
    },
    [&](const error& e) {
      VAST_IGNORE_UNUSED(e);
      VAST_ERROR("failed to spawn exporter:", self->system().render(e));
    }
  );
  if (!exp) {
    self->send_exit(*snk, exit_reason::user_shutdown);
    return 1;
  }
  // Start the exporter.
  self->send(exp, system::sink_atom::value, *snk);
  self->send(exp, system::run_atom::value);
  self->monitor(*snk);
  self->monitor(exp);
  auto rc = 0;
  auto stop = false;
  self->do_receive(
    [&](const down_msg& msg) {
      if (msg.source == node)  {
        VAST_DEBUG("received DOWN from node");
        self->send_exit(*snk, exit_reason::user_shutdown);
        self->send_exit(exp, exit_reason::user_shutdown);
        rc = 1;
      } else if (msg.source == exp) {
        VAST_DEBUG("received DOWN from exporter");
        self->send_exit(*snk, exit_reason::user_shutdown);
      } else if (msg.source == *snk) {
        VAST_DEBUG("received DOWN from sink");
        self->send_exit(exp, exit_reason::user_shutdown);
        rc = 1;
      } else {
        VAST_ASSERT(!"received DOWN from inexplicable actor");
      }
      stop = true;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG("got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM) {
        self->send_exit(exp, exit_reason::user_shutdown);
        self->send_exit(*snk, exit_reason::user_shutdown);
      }
    }
  ).until([&] { return stop; });
  return rc;
}

} // namespace vast::system
