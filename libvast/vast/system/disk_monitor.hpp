// SPDX-FileCopyrightText: (c) 2020 Tenzir GmbH <info@tenzir.com>
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include "vast/path.hpp"
#include "vast/system/actors.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>

namespace vast::system {

struct disk_monitor_state {
  /// The path to the database directory.
  std::filesystem::path dbdir;

  /// When current disk space is above the high water mark, stuff
  /// is deleted until we get below the low water mark.
  size_t high_water_mark;
  size_t low_water_mark;

  /// Whether an erasing run is currently in progress.
  bool purging;

  /// Node handle of the ARCHIVE.
  archive_actor archive;

  /// Node handle of the INDEX.
  index_actor index;

  /// The timespan between scans.
  std::chrono::seconds scan_interval;

  constexpr static const char* name = "disk_monitor";
};

/// Periodically scans the size of the database directory and deletes data
/// once it exceeds some threshold.
/// @param self The actor handle.
/// @param high_water Start erasing data if this limit is exceeded.
/// @param low_water Erase until this limit is no longer exceeded.
/// @param scan_interval The timespan between scans.
/// @param db_dir The path to the database directory.
/// @param archive The actor handle of the ARCHIVE.
/// @param index The actor handle of the INDEX.
disk_monitor_actor::behavior_type
disk_monitor(disk_monitor_actor::stateful_pointer<disk_monitor_state> self,
             size_t high_water, size_t low_water,
             std::chrono::seconds scan_interval,
             const std::filesystem::path& db_dir, archive_actor archive,
             index_actor index);

} // namespace vast::system
