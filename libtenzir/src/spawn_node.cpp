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

caf::expected<scope_linked<node_actor>>
spawn_node(caf::scoped_actor& self, const caf::settings& opts) {
  using namespace std::string_literals;
  // Fetch values from config.
  auto id = get_or(opts, "tenzir.node-id", defaults::node_id.data());
  auto db_dir
    = get_or(opts, "tenzir.state-directory", defaults::state_directory.data());
  auto detach_components = caf::get_or(opts, "tenzir.detach-components",
                                       defaults::detach_components);
  TENZIR_INFO("spawn node opts: {}", opts);
  auto default_components = caf::make_config_value_list(
    "catalog", "index", "importer", "eraser", "disk-monitor");
  auto maybe_components = get_or(opts, "tenzir.components", default_components);
  auto components = get_as<caf::config_value::list>(maybe_components);
  if (!components)
    return add_context(ec::unrecognized_option,
                       "tenzir.components must be a list of component names, "
                       "but got {}",
                       maybe_components);
  auto needs_db_directory = false;
  for (const auto& c : *components) {
    auto component = get_as<std::string>(c);
    TENZIR_INFO("{}", *component);
    if (*component == "index" || *component == "archive") {
      needs_db_directory = true;
      break;
    }
  }
  std::error_code err{};
  const auto abs_dir = std::filesystem::absolute(db_dir, err);
  if (err)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to get absolute path to "
                                       "state-directory {}: {}",
                                       db_dir, err.message()));
  auto pid_file = abs_dir / "pid.lock";
  if (needs_db_directory) {
    TENZIR_INFO("needs_db_directory");
    const auto dir_exists = std::filesystem::exists(abs_dir, err);
    if (!dir_exists) {
      if (auto created_dir = std::filesystem::create_directories(abs_dir, err);
          !created_dir)
        return caf::make_error(ec::filesystem_error,
                               fmt::format("unable to create db-directory {}: "
                                           "{}",
                                           abs_dir, err.message()));
    }
    if (const auto is_writable = ::access(abs_dir.c_str(), W_OK) == 0;
        !is_writable)
      return caf::make_error(
        ec::filesystem_error,
        "unable to write to state-directory:", abs_dir.string());
    // Acquire PID lock.
    TENZIR_DEBUG("node acquires PID lock {}", pid_file.string());
    if (auto err = detail::acquire_pid_file(pid_file))
      return err;
    // Remove old VERSION file if it exists. This can be removed once the
    // minimum partition version is >= 3.
    {
      auto err = std::error_code{};
      std::filesystem::remove(abs_dir / "VERSION", err);
      if (err)
        TENZIR_WARN("failed to remove outdated VERSION file: {}",
                    err.message());
    }
  }
  // Register self as the termination handler.
  auto signal_reflector
    = self->system().registry().get<signal_reflector_actor>("signal-reflector");
  self->send(signal_reflector, atom::subscribe_v);
  // Spawn the node.
  TENZIR_DEBUG("{} spawns local node: {}", __func__, id);
  // Pointer to the root command to node.
  auto detach_filesystem
    = detach_components ? detach_components::yes : detach_components::no;
  auto actor = self->spawn(node, id, abs_dir, detach_filesystem);
  actor->attach_functor([=, pid_file = std::move(pid_file),
                         &system = self->system()](
                          const caf::error&) -> caf::result<void> {
    TENZIR_DEBUG("node removes PID lock: {}", pid_file);
    // TODO: This works because the scope_linked framing around the actor handle
    //       sends an implicit exit message to the node in its destructor.
    //       In case we change this to RAII we need to add `scope_lock` like
    //       callback functionality to `scope_linked` instead.
    system.registry().erase("tenzir.node");
    if (needs_db_directory) {
      std::error_code err{};
      std::filesystem::remove_all(pid_file, err);
      if (err)
        return caf::make_error(ec::filesystem_error,
                               fmt::format("unable to remove pid file {} : {}",
                                           pid_file, err.message()));
    }
    return {};
  });

  self->system().registry().put("tenzir.node", actor);
  scope_linked<node_actor> node{std::move(actor)};
  // Logically everything below this comment should be part of the
  // start_command, but the `-N` option of the other commands requires
  // that we spawn components here.
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
  for (auto& c : *components) {
    auto component = get_as<std::string>(c);
    if (!component)
      return add_context(
        ec::unrecognized_option,
        "tenzir.component list element must be a string. Got {}", c);
    if (auto err = spawn_component(*component)) {
      TENZIR_ERROR("node failed to spawn {}: {}", component, err);
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

} // namespace tenzir
