//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/actors.hpp"
#include "tenzir/dbdir_size.hpp"
#include "tenzir/detail/flat_set.hpp"
#include "tenzir/option.hpp"
#include "tenzir/uuid.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>

namespace tenzir {

struct disk_monitor_state {
  struct blacklist_entry {
    tenzir::uuid id;
    caf::error error;
    friend bool operator<(const blacklist_entry&, const blacklist_entry&);
  };

  /// The path to the database directory.
  std::filesystem::path state_directory;

  /// The user-configurable behavior.
  disk_monitor_config config;

  /// The number of partitions that are scheduled for deletion and we expect to
  /// receive a response from.
  size_t pending_partitions = 0;

  /// Node handle of the INDEX.
  catalog_actor catalog;

  /// List of known-bad partitions
  detail::flat_set<blacklist_entry> blacklist;

  [[nodiscard]] bool purging() const;

  constexpr static const char* name = "disk-monitor";
};

/// Periodically scans the size of the database directory and deletes data
/// once it exceeds some threshold.
/// @param self The actor handle.
/// @param high_water Start erasing data if this limit is exceeded.
/// @param low_water Erase until this limit is no longer exceeded.
/// @param scan_interval The timespan between scans.
/// @param db_dir The path to the database directory.
/// @param catalog The actor handle of the CATALOG.
disk_monitor_actor::behavior_type
disk_monitor(disk_monitor_actor::stateful_pointer<disk_monitor_state> self,
             const disk_monitor_config& config,
             const std::filesystem::path& db_dir, catalog_actor catalog);

} // namespace tenzir
