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

#include <csignal>

#include <caf/actor_cast.hpp>
#include <caf/config_value.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"

#include "vast/system/accountant.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/tracker.hpp"

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

caf::message source_command([[maybe_unused]] const command& cmd,
                            caf::actor_system& sys, caf::actor src,
                            caf::settings& options,
                            command::argument_iterator begin,
                            command::argument_iterator end) {
  using namespace caf;
  using namespace std::chrono_literals;
  // Helper for blocking actor communication.
  scoped_actor self{sys};
  // Attempt to parse the remainder as an expression.
  if (begin != end) {
    auto expr = parse_expression(begin, end);
    if (!expr)
      return make_message(std::move(expr.error()));
    self->send(src, std::move(*expr));
  }
  // Get VAST node.
  auto node_opt = spawn_or_connect_to_node(self, "import.node", options,
                                           content(sys.config()));
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
               ? caf::get<caf::actor>(node_opt)
               : caf::get<scope_linked_actor>(node_opt).get();
  VAST_DEBUG(&cmd, "got node");
  // Start signal monitor.
  std::thread sig_mon_thread;
  auto guard = signal_monitor::run_guarded(sig_mon_thread, sys, 750ms, self);
  // Set defaults.
  caf::error err;
  // Connect source to importers.
  caf::actor importer;
  self->request(node, infinite, get_atom::value).receive(
    [&](const std::string& id, system::registry& reg) {
      // Assign accountant to source.
      VAST_DEBUG(&cmd, "assigns accountant from node", id, "to new source");
      auto er = reg.components[id].equal_range("accountant");
      if (er.first != er.second) {
        auto accountant = er.first->second.actor;
        self->send(src, actor_cast<accountant_type>(accountant));
      }
      // Assign IMPORTER to SOURCE and start streaming.
      er = reg.components[id].equal_range("importer");
      if (er.first == er.second) {
        err = make_error(ec::no_importer);
      } else if (reg.components[id].count("importer") > 1) {
        err = make_error(ec::unimplemented,
                         "multiple IMPORTER actors currently not supported");
      } else {
        VAST_DEBUG(&cmd, "connects to importer");
        importer = er.first->second.actor;
        self->send(src, system::sink_atom::value, importer);
      }
    },
    [&](error& e) {
      err = std::move(e);
    }
  );
  if (err)
    return make_message(std::move(err));
  // Start the source.
  bool stop = false;
  self->monitor(src);
  self->monitor(importer);
  // clang-format off
  self->do_receive(
    [&](const down_msg& msg) {
      if (msg.source == importer)  {
        VAST_DEBUG(&cmd, "received DOWN from node importer");
        self->send_exit(src, exit_reason::user_shutdown);
        err = ec::remote_node_down;
        stop = true;
      } else if (msg.source == src) {
        VAST_DEBUG(&cmd, "received DOWN from source");
        if (caf::get_or(options, "import.blocking", false))
          self->send(importer, subscribe_atom::value, flush_atom::value, self);
        else
          stop = true;
      } else {
        VAST_DEBUG(&cmd, "received unexpected DOWN from", msg.source);
        VAST_ASSERT(!"unexpected DOWN message");
      }
    },
    [&](flush_atom) {
      VAST_DEBUG(&cmd, "received flush from IMPORTER");
      stop = true;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG(&cmd, "got " << ::strsignal(signal));
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
