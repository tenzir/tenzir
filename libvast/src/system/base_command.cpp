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

#include "vast/system/base_command.hpp"

#include <caf/event_based_actor.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/stateful_actor.hpp>
#include <caf/io/middleman.hpp>

#ifdef VAST_USE_OPENSSL
#include <caf/openssl/all.hpp>
#endif

#include "vast/filesystem.hpp"
#include "vast/logger.hpp"

#include "vast/concept/parseable/vast/endpoint.hpp"

#include "vast/system/node.hpp"

using namespace caf;

namespace vast::system {

base_command::base_command(command* parent, std::string_view name)
  : command(parent, name),
    node_spawned_(false) {
  // nop
}

base_command::~base_command() {
  // nop
}

expected<actor> base_command::spawn_or_connect_to_node(scoped_actor& self,
                                                       const option_map& opts) {
  if (get_or<bool>(opts, "node", false))
    return spawn_node(self, opts);
  return connect_to_node(self, opts);
}

expected<actor> base_command::spawn_node(scoped_actor& self,
                                         const option_map& opts) {
  // Fetch node ID from config.
  auto id_opt = get<std::string>(opts, "id");
  if (!id_opt)
    return make_error(sec::invalid_argument, "ID missing in options map");
  auto id = std::move(*id_opt);
  // Fetch path to persistent state from config.
  auto dir_opt = get<std::string>(opts, "dir");
  if (!dir_opt)
    return make_error(sec::invalid_argument,
                      "Directory path missing in options map");
  auto dir = std::move(*dir_opt);
  auto abs_dir = path{dir}.complete();
  VAST_INFO("spawning local node:", id);
  // Pointer to the root command to system::node.
  auto node = self->spawn(system::node, id, abs_dir);
  node_spawned_ = true;
  if (!get_or<bool>(opts, "bare", false)) {
    // If we're not in bare mode, we spawn all core actors.
    auto spawn_component = [&](auto&&... xs) {
      return [&] {
        auto result = error{};
        auto args = make_message(std::move(xs)...);
        self->request(node, infinite, "spawn", std::move(args)).receive(
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
      VAST_ERROR(self->system().render(err));
      cleanup(node);
      return err;
    }
  }
  return node;
}

expected<actor> base_command::connect_to_node(scoped_actor& self,
                                              const option_map& opts) {
  // Fetch node ID from config.
  auto id_opt = get<std::string>(opts, "id");
  if (!id_opt)
    return make_error(sec::invalid_argument, "ID missing in options map");
  auto id = std::move(*id_opt);
  // Fetch path to persistent state from config.
  auto dir_opt = get<std::string>(opts, "dir");
  if (!dir_opt)
    return make_error(sec::invalid_argument,
                      "Directory path missing in options map");
  auto dir = std::move(*dir_opt);
  auto abs_dir = path{dir}.complete();
  // Fetch endpoint from config.
  auto endpoint_opt = get<std::string>(opts, "endpoint");
  if (!endpoint_opt)
    return make_error(sec::invalid_argument, "endpoint missing in options map");
  endpoint node_endpoint;
  if (!parsers::endpoint(*endpoint_opt, node_endpoint)) {
    std::string err = "invalid endpoint: ";
    err += *endpoint_opt;
    return make_error(sec::invalid_argument, std::move(err));
  }
  VAST_INFO("connect to remote node:", id);
  auto& sys_cfg = self->system().config();
  auto use_encryption = !sys_cfg.openssl_certificate.empty()
                        || !sys_cfg.openssl_key.empty()
                        || !sys_cfg.openssl_passphrase.empty()
                        || !sys_cfg.openssl_capath.empty()
                        || !sys_cfg.openssl_cafile.empty();
  auto host = node_endpoint.host;
  if (node_endpoint.host.empty())
    node_endpoint.host = "127.0.0.1";
  VAST_INFO("connecting to", node_endpoint.host << ':' << node_endpoint.port);
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

void base_command::cleanup(const actor& node) {
  if (node_spawned_)
    anon_send_exit(node, exit_reason::user_shutdown);
}

} // namespace vast::system
