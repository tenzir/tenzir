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

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/db_version.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/pid_file.hpp"
#include "vast/logger.hpp"
#include "vast/scope_linked.hpp"
#include "vast/system/node.hpp"

#include <caf/config_value.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include <filesystem>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace vast::system {

caf::expected<scope_linked<node_actor>>
spawn_node(caf::scoped_actor& self, const caf::settings& opts) {
  using namespace std::string_literals;
  // Fetch values from config.
  auto accounting = get_or(opts, "vast.enable-metrics", false);
  auto id = get_or(opts, "vast.node-id", defaults::system::node_id);
  auto db_dir
    = get_or(opts, "vast.db-directory", defaults::system::db_directory);
  auto abs_dir = path{db_dir}.complete();
  if (!exists(abs_dir)) {
    if (auto err = mkdir(abs_dir))
      return caf::make_error(ec::filesystem_error,
                             "unable to create db-directory:", abs_dir.str(),
                             err.context());
  }
  // Write VERSION file if it doesnt exist yet. Note that an empty db dir
  // often already exists before the node is initialized, e.g., when the log
  // output is written into the same directory.
  if (auto err = initialize_db_version(abs_dir))
    return err;
  if (auto version = read_db_version(std::filesystem::path{abs_dir.str()});
      version != db_version::latest) {
    VAST_INFO("Cannot start VAST, breaking changes detected in the database "
              "directory");
    auto reasons = describe_breaking_changes_since(version);
    return caf::make_error(
      ec::breaking_change,
      "breaking changes in the current database directory:", reasons);
  }
  if (!abs_dir.is_writable())
    return caf::make_error(ec::filesystem_error,
                           "unable to write to db-directory:", abs_dir.str());
  // Acquire PID lock.
  auto pid_file = std::filesystem::path{abs_dir.str()} / "pid.lock";
  VAST_DEBUG("{} acquires PID lock {}", node, pid_file.string());
  if (auto err = detail::acquire_pid_file(pid_file))
    return err;
  // Spawn the node.
  VAST_DEBUG("{} spawns local node: {}", __func__, id);
  auto shutdown_grace_period = defaults::system::shutdown_grace_period;
  if (auto str = caf::get_if<std::string>(&opts, "vast.shutdown-grace-"
                                                 "period")) {
    if (auto x = to<std::chrono::milliseconds>(*str))
      shutdown_grace_period = *x;
    else
      return x.error();
  }
  // Pointer to the root command to system::node.
  auto actor = self->spawn(system::node, id, abs_dir, shutdown_grace_period);
  actor->attach_functor([=, pid_file = std::move(pid_file)](
                          const caf::error&) -> caf::result<void> {
    VAST_DEBUG("node removes PID lock: {}", pid_file);
    std::error_code err{};
    std::filesystem::remove_all(pid_file, err);
    if (err)
      return caf::make_error(ec::filesystem_error,
                             fmt::format("unable to remove pid file {} : {}",
                                         pid_file, err.message()));
    return {};
  });
  scope_linked<node_actor> node{std::move(actor)};
  auto spawn_component = [&](std::string name) {
    caf::error result = caf::none;
    auto inv = invocation{opts, "spawn "s + std::move(name), {}};
    self->request(node.get(), caf::infinite, atom::spawn_v, std::move(inv))
      .receive(
        [&](caf::actor) {
          // nop
        },
        [&](caf::error err) { //
          result = std::move(err);
        });
    return result;
  };
  std::list components = {"type-registry", "archive", "index",
                          "importer",      "eraser",  "disk_monitor"};
  if (accounting)
    components.push_front("accountant");
  for (auto& c : components) {
    if (auto err = spawn_component(c)) {
      VAST_ERROR("node failed to spawn {}: {}", c, err);
      return err;
    }
  }
  return node;
}

} // namespace vast::system
