//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/option.hpp"
#include "tenzir/partition_paths.hpp"

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <string>
#include <unordered_map>

namespace tenzir {

struct disk_monitor_config {
  /// When current disk space is above the high water mark, stuff
  /// is deleted until we get below the low water mark.
  size_t high_water_mark = 0;
  size_t low_water_mark = 0;

  /// How many partitions to delete at once before re-checking the disk size,
  /// while erasing.
  size_t step_size = 1;

  /// The command to use to determine file size.
  Option<std::string> scan_binary = None{};

  /// The timespan between scans.
  std::chrono::seconds scan_interval = std::chrono::seconds{60};
};

/// Tests if the passed config options represent a valid disk budget.
caf::error validate(const disk_monitor_config&);

struct disk_usage {
  uint64_t bytes = 0;
  /// Partition files included in the measurement, including unadmitted ingest.
  std::unordered_map<uuid, uint64_t> partition_bytes = {};
  /// External commands have no file inventory. Reject their measurements if
  /// the partition inventory changed between the surrounding directory walks.
  bool stable = true;
};

/// Computes the size of the database directory and its partition inventory.
/// Note that this function may spawn an external process to perform the
/// computation.
caf::expected<disk_usage>
compute_dbdir_size(const partition_paths&, const disk_monitor_config&);

} // namespace tenzir
