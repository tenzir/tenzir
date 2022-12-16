//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/spawn_disk_monitor.hpp"

#include "vast/concept/parseable/vast/si.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/settings.hpp"
#include "vast/logger.hpp"
#include "vast/system/disk_monitor.hpp"
#include "vast/system/node.hpp"
#include "vast/system/node_control.hpp"
#include "vast/system/spawn_arguments.hpp"
#include "vast/uuid.hpp"

#include <caf/settings.hpp>
#include <caf/typed_event_based_actor.hpp>

namespace vast::system {

caf::expected<caf::actor>
spawn_disk_monitor(node_actor::stateful_pointer<node_state> self,
                   spawn_arguments& args) {
  VAST_TRACE_SCOPE("{}", VAST_ARG(args));
  auto [index] = self->state.registry.find<index_actor>();
  if (!index)
    return caf::make_error(ec::missing_component, "index");
  auto opts = args.inv.options;
  std::optional<std::string> command;
  if (auto cmd = caf::get_if<std::string>( //
        &opts, "vast.start.disk-budget-check-binary"))
    command = *cmd;
  auto hiwater = detail::get_bytesize(opts, "vast.start.disk-budget-high", 0);
  auto lowater = detail::get_bytesize(opts, "vast.start.disk-budget-low", 0);
  if (!hiwater)
    return hiwater.error();
  if (!lowater)
    return lowater.error();
  // Set low == high as the default value.
  if (!*lowater)
    *lowater = *hiwater;
  auto step_size = caf::get_or(opts, "vast.start.disk-budget-step-size",
                               defaults::system::disk_monitor_step_size);
  auto default_seconds
    = std::chrono::seconds{defaults::system::disk_scan_interval}.count();
  auto interval = caf::get_or(opts, "vast.start.disk-budget-check-interval",
                              default_seconds);
  struct disk_monitor_config config
    = {*hiwater, *lowater, step_size, command, std::chrono::seconds{interval}};
  if (auto error = validate(config))
    return error;
  if (!*hiwater) {
    if (command)
      VAST_WARN("'vast.start.disk-budget-check-binary' is configured but "
                "'vast.start.disk-budget-high' is unset; disk-monitor will not "
                "be spawned");
    else
      VAST_VERBOSE("'vast.start.disk-budget-high' is unset; disk-monitor will "
                   "not be spawned");
    return ec::no_error;
  }
  const auto db_dir = caf::get_or(opts, "vast.db-directory",
                                  defaults::system::db_directory.data());
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
  VAST_VERBOSE("{} spawned a disk monitor", *self);
  return caf::actor_cast<caf::actor>(handle);
}

} // namespace vast::system
