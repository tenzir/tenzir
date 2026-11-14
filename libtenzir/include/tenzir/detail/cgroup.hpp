//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"

#include <filesystem>

namespace tenzir::detail {

/// Where a cgroup v2 hierarchy is mounted.
struct cgroup2_mount {
  /// The cgroup that the mount point shows, as a path in the hierarchy. A
  /// container often sees only a subtree, such as that of its pod.
  std::filesystem::path root = {};
  /// Where the mount point is in the file system.
  std::filesystem::path mount_point = {};
};

/// The first cgroup v2 mount in the mount table of this process, or nothing if
/// there is none or the platform has no procfs.
auto find_cgroup2_mount() -> Option<cgroup2_mount>;

} // namespace tenzir::detail
