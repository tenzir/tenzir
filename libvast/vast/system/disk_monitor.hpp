//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <optional>

namespace vast::system {

struct disk_monitor_config {
  /// When current disk space is above the high water mark, stuff
  /// is deleted until we get below the low water mark.
  size_t high_water_mark = 0;
  size_t low_water_mark = 0;

  /// How many partitions to delete at once before re-checking the disk size,
  /// while erasing.
  size_t step_size = 1;

  /// The command to use to determine file size.
  std::optional<std::string> scan_binary = std::nullopt;

  /// The timespan between scans.
  std::chrono::seconds scan_interval = std::chrono::seconds{60};
};

/// Tests if the passed config options represent a valid disk monitor
/// configuration.
caf::error validate(const disk_monitor_config&);

struct disk_monitor_state {
  /// The path to the database directory.
  std::filesystem::path dbdir;

  disk_monitor_config config;

  /// Whether an erasing run is currently in progress.
  bool purging;

  /// The number of partitions that are scheduled for deletion and we expect to
  /// receive a response from.
  size_t pending_partitions = 0;

  /// Node handle of the INDEX.
  index_actor index;

  /// Computes the size of the database directory.
  /// Note that this function may spawn an external process to perform the
  /// computation.
  [[nodiscard]] caf::expected<size_t> compute_dbdir_size() const;

  constexpr static const char* name = "disk-monitor";
};

/// Periodically scans the size of the database directory and deletes data
/// once it exceeds some threshold.
/// @param self The actor handle.
/// @param high_water Start erasing data if this limit is exceeded.
/// @param low_water Erase until this limit is no longer exceeded.
/// @param scan_interval The timespan between scans.
/// @param db_dir The path to the database directory.
/// @param index The actor handle of the INDEX.
disk_monitor_actor::behavior_type
disk_monitor(disk_monitor_actor::stateful_pointer<disk_monitor_state> self,
             const disk_monitor_config& config,
             const std::filesystem::path& db_dir, index_actor index);

} // namespace vast::system
