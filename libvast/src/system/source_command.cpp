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

#include <caf/config_value.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/typed_event_based_actor.hpp>

#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"
#include "vast/schema.hpp"
#include "vast/scope_linked.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"

#include "vast/system/signal_monitor.hpp"
#include "vast/system/source.hpp"
#include "vast/system/spawn_or_connect_to_node.hpp"
#include "vast/system/tracker.hpp"

namespace vast::system {

namespace {

caf::expected<schema> load_schema_file(std::string& path) {
  if (path.empty())
    return make_error(ec::filesystem_error, "");
  auto str = load_contents(path);
  if (!str)
    return str.error();
  return to<schema>(*str);
}

caf::expected<expression> parse_expression(command::argument_iterator begin,
                                           command::argument_iterator end) {
  auto str = detail::join(begin, end, " ");
  auto expr = to<expression>(str);
  if (expr)
    expr = normalize_and_validate(*expr);
  return expr;
}

} // namespace <anonymous>

caf::message source_command(const command& cmd, caf::actor_system& sys,
                            caf::actor src, caf::config_value_map& options,
                            command::argument_iterator begin,
                            command::argument_iterator end) {
  using namespace caf;
  using namespace std::chrono_literals;
  VAST_UNUSED(cmd);
  // Helper for blocking actor communication.
  scoped_actor self{sys};
  // Supply an alternate schema, if requested.
  expected<vast::schema> schema{caf::none};
  if (auto sf = caf::get_if<std::string>(&options, "schema-file")) {
    if (caf::get_if<std::string>(&options, "schema"))
      return make_message(make_error(
        ec::invalid_configuration, "had both schema and schema-file provided"));
    schema = load_schema_file(*sf);
  } else if (auto sc = caf::get_if<std::string>(&options, "schema")) {
    schema = to<vast::schema>(*sc);
  }
  if (schema)
    self->send(src, put_atom::value, std::move(*schema));
  else if (schema.error())
    return make_message(std::move(schema.error()));
  // Attempt to parse the remainder as an expression.
  if (begin != end) {
    auto expr = parse_expression(begin, end);
    if (!expr)
      return make_message(std::move(expr.error()));
    self->send(src, std::move(*expr));
  }
  // Get VAST node.
  auto node_opt = spawn_or_connect_to_node(self, options);
  if (auto err = caf::get_if<caf::error>(&node_opt))
    return make_message(std::move(*err));
  auto& node = caf::holds_alternative<caf::actor>(node_opt)
               ? caf::get<caf::actor>(node_opt)
               : caf::get<scope_linked_actor>(node_opt).get();
  VAST_DEBUG(&cmd, "got node");
  /// Spawn an actor that takes care of CTRL+C and friends.
  auto sig_mon = self->spawn<detached>(system::signal_monitor, 750ms,
                                       actor{self});
  auto guard = caf::detail::make_scope_guard([&] {
    self->send_exit(sig_mon, exit_reason::user_shutdown);
  });
  // Set defaults.
  caf::error err;
  // Connect source to importers.
  caf::actor importer;
  self->request(node, infinite, get_atom::value).receive(
    [&](const std::string& id, system::registry& reg) {
      // Assign accountant to source.
      VAST_DEBUG(&cmd, "assigns accountant from node", id, "to new source");
      auto er = reg.components[id].equal_range("accountant");
      VAST_ASSERT(er.first != er.second);
      auto accountant = er.first->second.actor;
      self->send(src, actor_cast<accountant_type>(accountant));
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
  self->do_receive(
    [&](const down_msg& msg) {
      if (msg.source == node)  {
        VAST_DEBUG(&cmd, "received DOWN from node");
        self->send_exit(src, exit_reason::user_shutdown);
        err = ec::remote_node_down;
        stop = true;
      } else if (msg.source == src) {
        VAST_DEBUG(&cmd, "received DOWN from source");
        if (caf::get_or(options, "blocking", false))
          self->send(importer, subscribe_atom::value, flush_atom::value, self);
        else
          stop = true;
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
  if (err)
    return make_message(std::move(err));
  return caf::none;
}

} // namespace vast::system
