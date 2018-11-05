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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/parseable/vast/schema.hpp"

#include "vast/system/signal_monitor.hpp"
#include "vast/system/source.hpp"
#include "vast/system/tracker.hpp"

namespace vast::system {

source_command::source_command(command* parent, std::string_view name)
  : super(parent, name) {
  add_opt<std::string>("schema-file,s", "path to alternate schema");
  add_opt<std::string>("schema,S", "alternate schema as string");
}

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

caf::message source_command::run_impl(caf::actor_system& sys,
                                      const caf::config_value_map& options,
                                      argument_iterator begin,
                                      argument_iterator end) {
  using namespace caf;
  using namespace std::chrono_literals;
  // Helper for blocking actor communication.
  scoped_actor self{sys};
  // Spawn the source.
  auto src_opt = make_source(self, options);
  if (!src_opt)
    return wrap_error(std::move(src_opt.error()));
  auto src = std::move(*src_opt);
  // Supply an alternate schema, if requested.
  expected<vast::schema> schema{caf::none};
  if (auto sf = caf::get_if<std::string>(&options, "schema-file")) {
    if (caf::get_if<std::string>(&options, "schema"))
      return wrap_error(ec::invalid_configuration,
                        "had both schema and schema-file provided");
    schema = load_schema_file(*sf);
  } else if (auto sc = caf::get_if<std::string>(&options, "schema")) {
    schema = to<vast::schema>(*sc);
  }
  if (schema)
    self->send(src, put_atom::value, std::move(*schema));
  else if (schema.error())
    return wrap_error(std::move(schema.error()));
  // Attempt to parse the remainder as an expression.
  if (begin != end) {
    auto expr = parse_expression(begin, end);
    if (!expr)
      return wrap_error(std::move(expr.error()));
    self->send(src, std::move(*expr));
  }
  // Get VAST node.
  auto node_opt = spawn_or_connect_to_node(self, options);
  if (!node_opt)
    return wrap_error(std::move(node_opt.error()));
  auto node = std::move(*node_opt);
  VAST_DEBUG(this, "got node");
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
      auto er = reg.components[id].equal_range("importer");
      if (er.first == er.second) {
        err = make_error(ec::no_importer);
      } else if (reg.components[id].count("importer") > 1) {
        err = make_error(ec::unimplemented,
                         "multiple IMPORTER actors currently not supported");
      } else {
        VAST_DEBUG(this, "connects to importer");
        importer = er.first->second.actor;
        self->send(src, system::sink_atom::value, importer);
      }
    },
    [&](error& e) {
      err = std::move(e);
    }
  );
  if (err) {
    cleanup(node);
    return wrap_error(std::move(err));
  }
  // Start the source.
  bool stop = false;
  self->send(src, system::run_atom::value);
  self->monitor(src);
  self->do_receive(
    [&](const down_msg& msg) {
      if (msg.source == node)  {
        VAST_DEBUG(this, "received DOWN from node");
        self->send_exit(src, exit_reason::user_shutdown);
        err = ec::remote_node_down;
        stop = true;
      } else if (msg.source == src) {
        VAST_DEBUG(this, "received DOWN from source");
        if (caf::get_or(options, "blocking", false))
          self->send(importer, subscribe_atom::value, flush_atom::value, self);
        else
          stop = true;
      }
    },
    [&](flush_atom) {
      VAST_DEBUG(this, "received flush from IMPORTER");
      stop = true;
    },
    [&](system::signal_atom, int signal) {
      VAST_DEBUG(this, "got " << ::strsignal(signal));
      if (signal == SIGINT || signal == SIGTERM)
        self->send_exit(src, exit_reason::user_shutdown);
    }
  ).until(stop);
  cleanup(node);
  if (err)
    return wrap_error(std::move(err));
  return caf::none;
}

} // namespace vast::system
