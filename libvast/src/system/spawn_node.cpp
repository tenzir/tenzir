//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_node.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/time.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/pid_file.hpp"
#include "vast/logger.hpp"
#include "vast/plugin.hpp"
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
  auto id = get_or(opts, "vast.node-id", defaults::system::node_id.data());
  auto db_dir
    = get_or(opts, "vast.db-directory", defaults::system::db_directory.data());
  auto detach_components = caf::get_or(opts, "vast.detach-components",
                                       defaults::system::detach_components);
  std::error_code err{};
  const auto abs_dir = std::filesystem::absolute(db_dir, err);
  if (err)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to get absolute path to "
                                       "db-directory {}: {}",
                                       db_dir, err.message()));
  const auto dir_exists = std::filesystem::exists(abs_dir, err);
  if (!dir_exists) {
    if (auto created_dir = std::filesystem::create_directories(abs_dir, err);
        !created_dir)
      return caf::make_error(ec::filesystem_error,
                             fmt::format("unable to create db-directory {}: {}",
                                         abs_dir, err.message()));
  }
  if (const auto is_writable = ::access(abs_dir.c_str(), W_OK) == 0;
      !is_writable)
    return caf::make_error(
      ec::filesystem_error,
      "unable to write to db-directory:", abs_dir.string());
  // Acquire PID lock.
  auto pid_file = abs_dir / "pid.lock";
  VAST_DEBUG("node acquires PID lock {}", pid_file.string());
  if (auto err = detail::acquire_pid_file(pid_file))
    return err;
  // Remove old VERSION file if it exists. This can be removed once the minimum
  // partition version is >= 3.
  {
    auto err = std::error_code{};
    std::filesystem::remove(abs_dir / "VERSION", err);
    if (err)
      VAST_WARN("failed to remove outdated VERSION file: {}", err.message());
  }
  // Register self as the termination handler.
  auto signal_reflector
    = self->system().registry().get<signal_reflector_actor>("signal-reflector");
  self->send(signal_reflector, atom::subscribe_v);
  // Spawn the node.
  VAST_DEBUG("{} spawns local node: {}", __func__, id);
  // Pointer to the root command to system::node.
  auto detach_filesystem
    = detach_components ? detach_components::yes : detach_components::no;
  auto actor = self->spawn(system::node, id, abs_dir, detach_filesystem);
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
        [&](const caf::actor&) {
          // nop
        },
        [&](caf::error& err) { //
          result = std::move(err);
        });
    return result;
  };
  std::list components
    = {"catalog", "index", "importer", "eraser", "disk-monitor"};
  for (auto& c : components) {
    if (auto err = spawn_component(c)) {
      VAST_ERROR("node failed to spawn {}: {}", c, err);
      return err;
    }
  }
  caf::error error = caf::none;
  self
    ->request(node.get(), caf::infinite, atom::internal_v, atom::spawn_v,
              atom::plugin_v)
    .receive([]() { /* nop */ },
             [&](caf::error& err) {
               error = std::move(err);
             });
  if (error)
    return error;
  return node;
}

} // namespace vast::system
