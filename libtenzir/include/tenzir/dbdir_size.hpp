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

#include <chrono>
#include <cstddef>
#include <filesystem>
#include <string>

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

/// Tests if the passed config options represent a valid disk monitor
/// configuration.
caf::error validate(const disk_monitor_config&);

/// Computes the size of the database directory.
/// Note that this function may spawn an external process to perform the
/// computation.
caf::expected<size_t>
compute_dbdir_size(std::filesystem::path, const disk_monitor_config&);

} // namespace tenzir
