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

#include "vast/defaults.hpp"
#include "vast/detail/pid_file.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/node.hpp"

#include <caf/config_value.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include <string>
#include <unistd.h>
#include <vector>

namespace vast::system {

caf::expected<scope_linked_actor>
spawn_node(caf::scoped_actor& self, const caf::settings& opts) {
  using namespace std::string_literals;
  // Fetch values from config.
  auto accounting = !get_or(opts, "system.disable-metrics", false);
  auto id = get_or(opts, "system.node-id", defaults::system::node_id);
  auto db_dir
    = get_or(opts, "system.db-directory", defaults::system::db_directory);
  auto abs_dir = path{db_dir}.complete();
  if (!exists(abs_dir))
    if (auto res = mkdir(abs_dir); !res)
      return make_error(ec::filesystem_error,
                        "unable to create db-directory:", abs_dir.str());
  if (!abs_dir.is_writable())
    return make_error(ec::filesystem_error,
                      "unable to write to db-directory:", abs_dir.str());
  // Acquire PID lock.
  auto pid_file = abs_dir / "pid.lock";
  VAST_DEBUG_ANON(__func__, "acquires PID lock", pid_file.str());
  if (auto err = detail::acquire_pid_file(pid_file))
    return err;
  // Spawn the node.
  VAST_DEBUG_ANON(__func__, "spawns local node:", id);
  // Pointer to the root command to system::node.
  auto actor = self->spawn(system::node, id, abs_dir);
  actor->attach_functor([pid_file = std::move(pid_file)](const caf::error&) {
    VAST_DEBUG_ANON(__func__, "removes PID lock:", pid_file.str());
    rm(pid_file);
  });
  scope_linked_actor node{std::move(actor)};
  auto spawn_component = [&](std::string name) {
    caf::error result;
    auto inv = invocation{opts, "spawn "s + std::move(name), {}};
    self->request(node.get(), caf::infinite, std::move(inv))
      .receive([](const caf::actor&) { /* nop */ },
               [&](caf::error& e) { result = std::move(e); });
    return result;
  };
  std::list components
    = {"type-registry", "archive", "index", "importer", "eraser"};
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
