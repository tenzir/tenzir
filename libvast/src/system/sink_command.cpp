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

#include "vast/system/sink_command.hpp"

#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/accountant.hpp"
#include "vast/system/signal_monitor.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/tracker.hpp"

using namespace std::chrono_literals;
using namespace caf;

namespace vast::system {

caf::message sink_command(const command& cmd, actor_system& sys, caf::actor snk,
                          caf::settings& options,
                          command::argument_iterator first,
                          command::argument_iterator last) {
  VAST_UNUSED(cmd);
  // Read query from input file, STDIN or CLI arguments.
  std::string query;
  if (auto fname = caf::get_if<std::string>(&options, "read")) {
    // Sanity check.
    if (first != last) {
      auto err = make_error(ec::parse_error, "got a query on the command line "
                                             "but --read option is defined");
      return make_message(std::move(err));
    }
    std::ifstream f{*fname};
    if (!f) {
      auto err = make_error(ec::no_such_file, "unable to read from " + *fname);
      return make_message(std::move(err));
    }
    query.assign(std::istreambuf_iterator<char>(f),
                 std::istreambuf_iterator<char>());
  } else if (first == last) {
    // Read query from STDIN.
    query.assign(std::istreambuf_iterator<char>(std::cin),
                 std::istreambuf_iterator<char>());
  } else {
    query = *first;
    for (auto i = std::next(first); i != last; ++i) {
      query += ' ';
      query += *i;
    }
  }
  if (query.empty()) {
    auto err = make_error(ec::invalid_query);
    return make_message(std::move(err));
  }
  // Get a convenient and blocking way to interact with actors.
  scoped_actor self{sys};
  // Get VAST node.
  auto node_opt = spawn_or_connect_to_node(self, options);
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return caf::make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
               ? caf::get<caf::actor>(node_opt)
               : caf::get<scope_linked_actor>(node_opt).get();
  VAST_ASSERT(node != nullptr);
  /// Spawn an actor that takes care of CTRL+C and friends.
  auto sig_mon = self->spawn<detached>(system::signal_monitor, 750ms, self);
  auto guard = caf::detail::make_scope_guard([&] {
    self->send_exit(sig_mon, exit_reason::user_shutdown);
  });
  // Spawn exporter at the node.
  actor exp;
  std::vector<std::string> args{"spawn", "exporter"};
  args.emplace_back(std::move(query));
  VAST_DEBUG(&cmd, "spawns exporter with parameters:", args);
  error err;
  self->request(node, infinite, std::move(args), options).receive(
    [&](actor& a) {
      exp = std::move(a);
      if (!exp)
        err = make_error(ec::invalid_result, "remote spawn returned nullptr");
    },
    [&](error& e) {
      err = std::move(e);
    }
  );
  if (err) {
    self->send_exit(snk, exit_reason::user_shutdown);
    return caf::make_message(std::move(err));
  }
  self->request(node, infinite, get_atom::value)
    .receive(
      [&](const std::string& id, system::registry& reg) {
        // Assign accountant to sink.
        VAST_DEBUG(&cmd, "assigns accountant from node", id, "to new sink");
        auto er = reg.components[id].find("accountant");
        if (er != reg.components[id].end()) {
          auto accountant = er->second.actor;
          self->send(snk, actor_cast<accountant_type>(accountant));
        }
      },
      [&](error& e) { err = std::move(e); });
  if (err) {
    self->send_exit(snk, exit_reason::user_shutdown);
    return caf::make_message(std::move(err));
  }
  // Start the exporter.
  self->send(exp, system::sink_atom::value, snk);
  self->send(exp, system::run_atom::value);
  self->monitor(snk);
  self->monitor(exp);
  auto stop = false;
  self->do_receive(
    [&](down_msg& msg) {
      if (msg.source == node)  {
        VAST_DEBUG_ANON(__func__, "received DOWN from node");
        self->send_exit(snk, exit_reason::user_shutdown);
        self->send_exit(exp, exit_reason::user_shutdown);
      } else if (msg.source == exp) {
        VAST_DEBUG(&cmd, "received DOWN from exporter");
        self->send_exit(snk, exit_reason::user_shutdown);
      } else if (msg.source == snk) {
        VAST_DEBUG(&cmd, "received DOWN from sink");
        self->send_exit(exp, exit_reason::user_shutdown);
      } else {
        VAST_ASSERT(!"received DOWN from inexplicable actor");
      }
      if (msg.reason) {
        VAST_WARNING(
          &cmd, "received error message:", self->system().render(msg.reason));
        err = std::move(msg.reason);
      }
      stop = true;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG(&cmd, "got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM) {
        self->send_exit(exp, exit_reason::user_shutdown);
        self->send_exit(snk, exit_reason::user_shutdown);
      }
    }
  ).until([&] { return stop; });
  if (err)
    return caf::make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
