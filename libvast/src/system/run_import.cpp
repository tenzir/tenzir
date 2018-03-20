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

#include "vast/system/run_import.hpp"

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

run_import::run_import(command* parent, std::string_view name)
  : base_command(parent, name) {
  // nop
}

int run_import::run_impl(actor_system& sys, opt_map& options,
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
  // Set defaults.
  auto rc = 1;
  auto stop = false;
  // Spawn a source.
  auto opts = system::options{args, {}, {}};
  auto src = system::spawn_source(actor_cast<local_actor*>(self), opts);
  if (!src) {
    VAST_ERROR("failed to spawn source:", sys.render(src.error()));
    return rc;
  }
  // Connect source to importers.
  self->request(node, infinite, get_atom::value).receive(
    [&](const std::string& id, system::registry& reg) {
      auto er = reg.components[id].equal_range("importer");
      if (er.first == er.second) {
        VAST_ERROR("no importers available at node", id);
        stop = true;
      } else {
        VAST_DEBUG("connecting source to importers");
        for (auto i = er.first; i != er.second; ++i)
          self->send(*src, system::sink_atom::value, i->second.actor);
      }
    },
    [&](const error& e) {
      VAST_IGNORE_UNUSED(e);
      VAST_ERROR(self->system().render(e));
      stop = true;
    }
  );
  if (stop)
    return rc;
  // Start the source.
  rc = 0;
  self->send(*src, system::run_atom::value);
  self->monitor(*src);
  self->do_receive(
    [&](const down_msg& msg) {
      if (msg.source == node)  {
        VAST_DEBUG("received DOWN from node");
        self->send_exit(*src, exit_reason::user_shutdown);
        rc = 1;
      } else if (msg.source == *src) {
        VAST_DEBUG("received DOWN from source");
      }
      stop = true;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG("got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM)
        self->send_exit(*src, exit_reason::user_shutdown);
    }
  ).until([&] { return stop; });
  return rc;
}

} // namespace vast::system
