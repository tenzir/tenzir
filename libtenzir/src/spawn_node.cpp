//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/spawn_node.hpp"

#include "tenzir/concept/parseable/tenzir/time.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/pid_file.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/node.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/scope_linked.hpp"

#include <caf/config_value.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/settings.hpp>

#include <filesystem>
#include <string>
#include <system_error>
#include <unistd.h>
#include <vector>

namespace tenzir {

caf::expected<scope_linked<node_actor>> spawn_node(caf::scoped_actor& self) {
  using namespace std::string_literals;
  const auto& opts = content(self->system().config());
  // Fetch values from config.
  auto id = get_or(opts, "tenzir.node-id", defaults::node_id.data());
  auto db_dir
    = get_or(opts, "tenzir.state-directory", defaults::state_directory.data());
  std::error_code err{};
  const auto abs_dir = std::filesystem::absolute(db_dir, err);
  if (err)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to get absolute path to "
                                       "state-directory {}: {}",
                                       db_dir, err.message()));
  const auto dir_exists = std::filesystem::exists(abs_dir, err);
  if (!dir_exists) {
    if (auto created_dir = std::filesystem::create_directories(abs_dir, err);
        !created_dir)
      return caf::make_error(ec::filesystem_error,
                             fmt::format("unable to create state-directory {}: "
                                         "{}",
                                         abs_dir, err.message()));
  }
  if (const auto is_writable = ::access(abs_dir.c_str(), W_OK) == 0;
      !is_writable)
    return caf::make_error(
      ec::filesystem_error,
      "unable to write to state-directory:", abs_dir.string());
  // Acquire PID lock.
  auto pid_file = abs_dir / "pid.lock";
  TENZIR_DEBUG("node acquires PID lock {}", pid_file.string());
  if (auto err = detail::acquire_pid_file(pid_file))
    return err;
  // Remove old VERSION file if it exists. This can be removed once the minimum
  // partition version is >= 3.
  {
    std::filesystem::remove(abs_dir / "VERSION", err);
    if (err)
      TENZIR_WARN("failed to remove outdated VERSION file: {}", err.message());
  }
  // Register self as the termination handler.
  auto signal_reflector
    = self->system().registry().get<signal_reflector_actor>("signal-reflector");
  self->send(signal_reflector, atom::subscribe_v);
  // Wipe the contents of the old cache directory.
  {
    auto cache_directory = get_if<std::string>(&opts, "tenzir.cache-directory");
    if (cache_directory && std::filesystem::exists(*cache_directory)) {
      for (auto const& item :
           std::filesystem::directory_iterator{*cache_directory}) {
        std::filesystem::remove_all(item.path(), err);
        if (err) {
          TENZIR_WARN("failed to remove {} from cache: {}", *cache_directory,
                      err);
        }
      }
    }
  }
  // Spawn the node.
  TENZIR_DEBUG("{} spawns local node: {}", __func__, id);
  // Pointer to the root command to node.
  auto actor = self->spawn(node, id, abs_dir);
  actor->attach_functor([=, pid_file = std::move(pid_file),
                         &system = self->system()](
                          const caf::error&) -> caf::result<void> {
    TENZIR_DEBUG("node removes PID lock: {}", pid_file);
    // TODO: This works because the scope_linked framing around the actor handle
    //       sends an implicit exit message to the node in its destructor.
    //       In case we change this to RAII we need to add `scope_lock` like
    //       callback functionality to `scope_linked` instead.
    system.registry().erase("tenzir.node");
    std::error_code err{};
    std::filesystem::remove_all(pid_file, err);
    if (err)
      return caf::make_error(ec::filesystem_error,
                             fmt::format("unable to remove pid file {} : {}",
                                         pid_file, err.message()));
    return {};
  });
  self->system().registry().put("tenzir.node", actor);
  return scope_linked<node_actor>{std::move(actor)};
}

} // namespace tenzir
