//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/cgroup.hpp"

#include "tenzir/config.hpp"

#if TENZIR_LINUX
#  include <pfs/procfs.hpp>
#endif

#include <exception>

namespace tenzir::detail {

auto find_cgroup2_mount() -> Option<cgroup2_mount> {
#if TENZIR_LINUX
  try {
    for (const auto& mount : pfs::procfs{}.get_task().get_mountinfo()) {
      if (mount.filesystem_type != "cgroup2") {
        continue;
      }
      return cgroup2_mount{
        .root = mount.root,
        .mount_point = mount.point,
      };
    }
  } catch (const std::exception&) {
  }
#endif
  return None{};
}

} // namespace tenzir::detail
