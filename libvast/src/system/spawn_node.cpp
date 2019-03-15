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

#include "vast/system/spawn_node.hpp"

#include <string>
#include <vector>

#include <caf/config_value.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include "vast/defaults.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/node.hpp"

namespace vast::system {

expected<scope_linked_actor> spawn_node(caf::scoped_actor& self,
                                        const caf::settings& opts) {
  // Fetch values from config.
  auto accounting = !get_or(opts, "vast.disable-accounting", false);
  auto id = get_or(opts, "vast.id", defaults::command::node_id);
  auto dir = get_or(opts, "vast.directory", defaults::command::directory);
  auto abs_dir = path{dir}.complete();
  VAST_DEBUG_ANON(__func__, "spawns local node:", id);
  // Pointer to the root command to system::node.
  scope_linked_actor node{self->spawn(system::node, id, abs_dir)};
  auto spawn_component = [&](std::string name) {
    caf::error result;
    std::vector<std::string> args{"spawn", std::move(name)};
    self->request(node.get(), caf::infinite, std::move(args))
      .receive([](const caf::actor&) { /* nop */ },
               [&](caf::error& e) { result = std::move(e); });
    return result;
  };
  std::list components = {"consensus", "archive", "index", "importer"};
  if (accounting)
    components.push_front("accountant");
  for (auto& c : components) {
    if (auto err = spawn_component(c)) {
      VAST_ERROR(self, self->system().render(err));
      return err;
    }
  }
  return node;
}

} // namespace vast::system
