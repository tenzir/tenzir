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

#ifndef VAST_SYSTEM_RUN_READER_HPP
#define VAST_SYSTEM_RUN_READER_HPP

#include "vast/system/reader_command_base.hpp"

#include <csignal>
#include <memory>
#include <string>
#include <string_view>

#include <caf/scoped_actor.hpp>
#include <caf/typed_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "vast/expression.hpp"
#include "vast/logger.hpp"

#include "vast/system/node_command.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/source.hpp"
#include "vast/system/tracker.hpp"

#include "vast/concept/parseable/to.hpp"

#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"

namespace vast::system {

int reader_command_base::run_impl(caf::actor_system& sys,
                                  const caf::config_value_map& options,
                                  argument_iterator begin,
                                  argument_iterator end) {
  using namespace caf;
  using namespace std::chrono_literals;
  // Helper for blocking actor communication.
  scoped_actor self{sys};
  // Spawn the source.
  auto src_opt = make_source(self, options, begin, end);
  if (!src_opt) {
    std::cerr << "unable to spawn source: " << sys.render(src_opt.error())
              << std::endl;
    return EXIT_FAILURE;
  }
  auto src = std::move(*src_opt);
  // Get VAST node.
  auto node_opt = spawn_or_connect_to_node(self, options);
  if (!node_opt)
    return EXIT_FAILURE;
  auto node = std::move(*node_opt);
  VAST_INFO("got node");
  /// Spawn an actor that takes care of CTRL+C and friends.
  auto sig_mon = self->spawn<detached>(system::signal_monitor, 750ms,
                                       actor{self});
  auto guard = caf::detail::make_scope_guard([&] {
    self->send_exit(sig_mon, exit_reason::user_shutdown);
  });
  // Set defaults.
  int rc = EXIT_FAILURE;
  auto stop = false;
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
          self->send(src, system::sink_atom::value, i->second.actor);
      }
    },
    [&](const error& e) {
      VAST_IGNORE_UNUSED(e);
      VAST_ERROR(self->system().render(e));
      stop = true;
    }
  );
  if (stop) {
    cleanup(node);
    return rc;
  }
  // Start the source.
  rc = EXIT_SUCCESS;
  self->send(src, system::run_atom::value);
  self->monitor(src);
  self->do_receive(
    [&](const down_msg& msg) {
      if (msg.source == node)  {
        VAST_DEBUG("received DOWN from node");
        self->send_exit(src, exit_reason::user_shutdown);
        rc = EXIT_FAILURE;
      } else if (msg.source == src) {
        VAST_DEBUG("received DOWN from source");
      }
      stop = true;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG("got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM)
        self->send_exit(src, exit_reason::user_shutdown);
    }
  ).until([&] { return stop; });
  cleanup(node);
  return rc;
}

} // namespace vast::system

#endif
