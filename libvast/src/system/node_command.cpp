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

#include "vast/system/node_command.hpp"

#include <caf/actor_system_config.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/io/middleman.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>

#ifdef VAST_USE_OPENSSL
#include <caf/openssl/all.hpp>
#endif

#include "vast/concept/parseable/vast/endpoint.hpp"
#include "vast/defaults.hpp"
#include "vast/filesystem.hpp"
#include "vast/logger.hpp"
#include "vast/system/node.hpp"

using namespace caf;

namespace vast::system {

node_command::node_command(command* parent) : command(parent) {
  // nop
}

node_command::~node_command() {
  // nop
}

auto node_command::spawn_or_connect_to_node(scoped_actor& self,
                                            const caf::config_value_map& opts)
-> node_factory_result {
  auto convert = [](auto&& result) -> node_factory_result {
    if (result)
      return std::move(*result);
    else
      return std::move(result.error());
  };
  if (get_or<bool>(opts, "node", false))
    return convert(spawn_node(self, opts));
  return convert(connect_to_node(self, opts));
}

expected<scope_linked_actor>
node_command::spawn_node(scoped_actor& self,
                         const caf::config_value_map& opts) {
  // Fetch values from config.
  auto id = get_or(opts, "id", defaults::command::node_id);
  auto dir = get_or(opts, "dir", defaults::command::directory);
  auto abs_dir = path{dir}.complete();
  VAST_DEBUG(this, "spawns local node:", id);
  // Pointer to the root command to system::node.
  scope_linked_actor node{self->spawn(system::node, id, abs_dir)};
  auto spawn_component = [&](auto&&... xs) {
    return [&] {
      auto result = error{};
      auto args = make_message(std::move(xs)...);
      self->request(node.get(), infinite, "spawn", std::move(args)).receive(
        [](const actor&) { /* nop */ },
        [&](error& e) { result = std::move(e); }
      );
      return result;
    };
  };
  auto err = error::eval(
    spawn_component("metastore"),
    spawn_component("archive"),
    spawn_component("index"),
    spawn_component("importer")
  );
  if (err) {
    VAST_ERROR(self, self->system().render(err));
    return err;
  }
  return node;
}

expected<actor>
node_command::connect_to_node(scoped_actor& self,
                              const caf::config_value_map& opts) {
  // Fetch values from config.
  auto id = get_or(opts, "id", defaults::command::node_id);
  auto dir = get_or(opts, "dir", defaults::command::directory);
  auto abs_dir = path{dir}.complete();
  auto endpoint_str = get_or(opts, "endpoint", defaults::command::endpoint);
  endpoint node_endpoint;
  if (!parsers::endpoint(endpoint_str, node_endpoint)) {
    std::string err = "invalid endpoint: ";
    err += endpoint_str;
    return make_error(sec::invalid_argument, std::move(err));
  }
  VAST_DEBUG(self, "connects to remote node:", id);
  auto& sys_cfg = self->system().config();
  auto use_encryption = !sys_cfg.openssl_certificate.empty()
                        || !sys_cfg.openssl_key.empty()
                        || !sys_cfg.openssl_passphrase.empty()
                        || !sys_cfg.openssl_capath.empty()
                        || !sys_cfg.openssl_cafile.empty();
  auto host = node_endpoint.host;
  if (node_endpoint.host.empty())
    node_endpoint.host = "127.0.0.1";
  VAST_INFO(self, "connects to", node_endpoint.host << ':' << node_endpoint.port);
  if (use_encryption) {
#ifdef VAST_USE_OPENSSL
    return openssl::remote_actor(self->system(), node_endpoint.host,
                                 node_endpoint.port);
#else
    return make_error(ec::unspecified, "not compiled with OpenSSL support");
#endif
  }
  auto& mm = self->system().middleman();
  return mm.remote_actor(node_endpoint.host, node_endpoint.port);
}

} // namespace vast::system
