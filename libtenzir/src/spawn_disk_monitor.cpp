//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/spawn_disk_monitor.hpp"

#include "tenzir/concept/parseable/tenzir/si.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/settings.hpp"
#include "tenzir/disk_monitor.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/node.hpp"
#include "tenzir/node_control.hpp"
#include "tenzir/spawn_arguments.hpp"
#include "tenzir/uuid.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace tenzir {

caf::expected<caf::actor>
spawn_disk_monitor(node_actor::stateful_pointer<node_state> self,
                   spawn_arguments& args) {
  TENZIR_TRACE_SCOPE("{}", TENZIR_ARG(args));
  auto [index] = self->state.registry.find<index_actor>();
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  auto opts = args.inv.options;
  std::optional<std::string> command;
  if (auto cmd = caf::get_if<std::string>( //
        &opts, "tenzir.start.disk-budget-check-binary"))
    command = *cmd;
  auto hiwater = detail::get_bytesize(opts, "tenzir.start.disk-budget-high", 0);
  auto lowater = detail::get_bytesize(opts, "tenzir.start.disk-budget-low", 0);
  if (!hiwater)
    return hiwater.error();
  if (!lowater)
    return lowater.error();
  // Set low == high as the default value.
  if (!*lowater)
    *lowater = *hiwater;
  auto step_size = caf::get_or(opts, "tenzir.start.disk-budget-step-size",
                               defaults::disk_monitor_step_size);
  auto default_seconds
    = std::chrono::seconds{defaults::disk_scan_interval}.count();
  auto interval = caf::get_or(opts, "tenzir.start.disk-budget-check-interval",
                              default_seconds);
  struct disk_monitor_config config
    = {*hiwater, *lowater, step_size, command, std::chrono::seconds{interval}};
  if (auto error = validate(config))
    return error;
  if (!*hiwater) {
    if (command)
      TENZIR_WARN(
        "'tenzir.start.disk-budget-check-binary' is configured but "
        "'tenzir.start.disk-budget-high' is unset; disk-monitor will not "
        "be spawned");
    else
      TENZIR_VERBOSE(
        "'tenzir.start.disk-budget-high' is unset; disk-monitor will "
        "not be spawned");
    return ec::no_error;
  }
  const auto db_dir = caf::get_or(opts, "tenzir.state-directory",
                                  defaults::state_directory.data());
  const auto db_dir_path = std::filesystem::path{db_dir};
  std::error_code err{};
  const auto db_dir_abs = std::filesystem::absolute(db_dir_path, err);
  if (err)
    return caf::make_error(ec::filesystem_error, "could not make absolute path "
                                                 "to database directory");
  if (!std::filesystem::exists(db_dir_abs))
    return caf::make_error(ec::filesystem_error, "could not find database "
                                                 "directory");
  auto handle = self->spawn(disk_monitor, config, db_dir_abs, index);
  TENZIR_VERBOSE("{} spawned a disk monitor", *self);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace tenzir
