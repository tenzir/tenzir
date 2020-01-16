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

#include "vast/system/source_command.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/atoms.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/tracker.hpp"

#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include <csignal>

namespace vast::system {

namespace {

caf::expected<expression> parse_expression(command::argument_iterator begin,
                                           command::argument_iterator end) {
  auto str = detail::join(begin, end, " ");
  auto expr = to<expression>(str);
  if (expr)
    expr = normalize_and_validate(*expr);
  return expr;
}

} // namespace <anonymous>

caf::message source_command(const command::invocation& invocation,
                            caf::actor_system& sys, caf::actor src) {
  using namespace caf;
  using namespace std::chrono_literals;
  // Helper for blocking actor communication.
  scoped_actor self{sys};
  // Attempt to parse the remainder as an expression.
  if (!invocation.arguments.empty()) {
    auto expr = parse_expression(invocation.arguments.begin(),
                                 invocation.arguments.end());
    if (!expr)
      return make_message(std::move(expr.error()));
    self->send(src, std::move(*expr));
  }
  // Get VAST node.
  auto node_opt
    = spawn_or_connect_to_node(self, invocation.options, content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
               ? caf::get<caf::actor>(node_opt)
               : caf::get<scope_linked_actor>(node_opt).get();
  VAST_DEBUG(invocation.full_name, "got node");
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = system::signal_monitor::run_guarded(
    sig_mon_thread, sys, defaults::system::signal_monitoring_interval, self);
  // Get node components.
  auto components
    = get_node_component<accountant_atom, importer_atom>(self, node);
  if (!components)
    return make_message(components.error());
  if (auto& accountant = (*components)[0]) {
    VAST_DEBUG(invocation.full_name, "assigns accountant to source");
    self->send(src, actor_cast<accountant_type>(*accountant));
  }
  // Connect source to importer.
  auto& importer = (*components)[1];
  if (!importer)
    return make_message(importer.error());
  VAST_DEBUG(invocation.full_name, "connects to importer");
  self->send(src, system::sink_atom::value, *importer);
  // Start the source.
  caf::error err;
  bool stop = false;
  self->monitor(src);
  self->monitor(*importer);
  // clang-format off
  self->do_receive(
    [&](const down_msg& msg) {
      if (msg.source == *importer)  {
        VAST_DEBUG(invocation.full_name, "received DOWN from node importer");
        self->send_exit(src, exit_reason::user_shutdown);
        err = ec::remote_node_down;
        stop = true;
      } else if (msg.source == src) {
        VAST_DEBUG(invocation.full_name, "received DOWN from source");
        if (caf::get_or(invocation.options, "import.blocking", false))
          self->send(*importer, subscribe_atom::value, flush_atom::value, self);
        else
          stop = true;
      } else {
        VAST_DEBUG(invocation.full_name, "received unexpected DOWN from", msg.source);
        VAST_ASSERT(!"unexpected DOWN message");
      }
    },
    [&](flush_atom) {
      VAST_DEBUG(invocation.full_name, "received flush from IMPORTER");
      stop = true;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG(invocation.full_name, "got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM)
        self->send_exit(src, exit_reason::user_shutdown);
    }
  ).until(stop);
  // clang-format on
  if (err)
    return make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
