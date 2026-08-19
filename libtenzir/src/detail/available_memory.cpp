//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/available_memory.hpp"

#include "tenzir/config.hpp"
#include "tenzir/detail/saturating_arithmetic.hpp"

#if defined(__APPLE__) && __has_include(<mach/mach.h>)
#  include <mach/mach.h>
#  include <mach/mach_host.h>

#  include <unistd.h>
#endif

#if TENZIR_LINUX
#  include <pfs/procfs.hpp>
#endif

#include <algorithm>
#include <exception>
#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>

namespace tenzir::detail {

namespace {

auto read_memory_value(const std::filesystem::path& path) -> Option<uint64_t> {
  auto file = std::ifstream{path};
  auto value = std::string{};
  file >> value;
  if (value.empty() or value == "max") {
    return None{};
  }
  try {
    return std::stoull(value);
  } catch (const std::exception&) {
    return None{};
  }
}

/// Parses the `key value` lines of a cgroup `memory.stat` file.
auto read_memory_stat(std::filesystem::path const& path)
  -> std::unordered_map<std::string, uint64_t> {
  auto result = std::unordered_map<std::string, uint64_t>{};
  auto file = std::ifstream{path};
  auto key = std::string{};
  auto value = uint64_t{};
  while (file >> key >> value) {
    result.emplace(key, value);
  }
  return result;
}

/// Returns the part of a cgroup's charged memory that the kernel can reclaim
/// without swapping, i.e., evictable clean page cache and reclaimable slab.
/// Dirty pages and pages under writeback are excluded because they need
/// writeback first; they sit on the file LRU lists, so they must be subtracted
/// rather than merely left out. The counters cover the whole subtree, so cache
/// that a descendant's `memory.min` protects from reclaim counts as reclaimable
/// here; telling the two apart would mean walking that subtree and reproducing
/// the kernel's protection distribution on every call.
auto reclaimable_memory(std::filesystem::path const& dir) -> uint64_t {
  auto const stat = read_memory_stat(dir / "memory.stat");
  auto const get = [&](std::string const& key) -> uint64_t {
    if (auto const it = stat.find(key); it != stat.end()) {
      return it->second;
    }
    return 0;
  };
  // cgroup v1 reports every counter twice: once for this cgroup alone and once
  // for its subtree, prefixed with `total_`. All counters must come from the
  // same scope as the charge they are subtracted from, because pairing a local
  // counter with a subtree one treats whatever the descendants hold as
  // reclaimable. The prefixed counters are reported either way, so their mere
  // presence says nothing about the scope of `memory.usage_in_bytes`: that
  // covers the subtree only where hierarchical accounting is enabled. Modern
  // kernels refuse to disable it, and where the file is missing entirely there
  // is nothing to suggest a local charge. cgroup v2 has no `total_` variants;
  // its counters always cover the subtree.
  auto const v1 = stat.contains("total_cache");
  auto const subtree
    = v1 and read_memory_value(dir / "memory.use_hierarchy").value_or(1) != 0;
  auto const key = [&](char const* v2, char const* local) -> std::string {
    if (not v1) {
      return v2;
    }
    return subtree ? "total_" + std::string{local} : local;
  };
  auto const dirty = get(key("file_dirty", "dirty"));
  auto const writeback = get(key("file_writeback", "writeback"));
  auto unreclaimable = saturating_add(dirty, writeback);
  auto cache = uint64_t{};
  // Pages pinned with `mlock` move to the unevictable LRU but keep counting
  // towards the page cache total, so the file LRU lists are what the kernel can
  // actually reclaim. They hold only pages that are not swap-backed, which
  // leaves shmem out by construction.
  auto const inactive_file = key("inactive_file", "inactive_file");
  if (stat.contains(inactive_file)) {
    cache = saturating_add(get(key("active_file", "active_file")),
                           get(inactive_file));
  } else {
    // Kernels that omit the lists leave only the aggregate page cache counter,
    // which does include shmem as well as pinned pages. Subtracting the
    // unevictable counter can understate what is reclaimable, because it also
    // covers pinned anonymous pages that the page cache total never included in
    // the first place.
    cache = saturating_sub(get(key("file", "cache")),
                           get(key("unevictable", "unevictable")));
    unreclaimable = saturating_add(unreclaimable, get(key("shmem", "shmem")));
  }
  // Only cgroup v2 breaks the slab down by reclaimability.
  return saturating_add(saturating_sub(cache, unreclaimable),
                        get("slab_reclaimable"));
}

} // namespace

auto read_cgroup_memory_available(std::filesystem::path const& dir)
  -> Option<uint64_t> {
  auto current = read_memory_value(dir / "memory.current");
  auto max = read_memory_value(dir / "memory.max");
  if (not current or not max) {
    current = read_memory_value(dir / "memory.usage_in_bytes");
    max = read_memory_value(dir / "memory.limit_in_bytes");
  }
  if (not current or not max) {
    return None{};
  }
  static constexpr auto unlimited_cgroup_limit = uint64_t{1} << 60;
  if (*max >= unlimited_cgroup_limit) {
    return None{};
  }
  // Page cache the kernel reclaims under pressure is not memory we hold, so it
  // must not shrink what we consider available. Without this, mmap-heavy work
  // like the partition transformer's input loading starves its own budget.
  auto const consumed = saturating_sub(*current, reclaimable_memory(dir));
  return saturating_sub(*max, consumed);
}

namespace {

#if TENZIR_LINUX

auto relative_cgroup_path(std::string_view raw) -> std::filesystem::path {
  while (raw.starts_with('/')) {
    raw.remove_prefix(1);
  }
  return std::filesystem::path{std::string{raw}};
}

struct cgroup2_mount {
  std::filesystem::path root = {};
  std::filesystem::path mount_point = {};
};

auto find_cgroup2_mount() -> Option<cgroup2_mount> {
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
  return None{};
}

auto path_stays_below_root(const std::filesystem::path& path) -> bool {
  for (const auto& element : path) {
    if (element == "..") {
      return false;
    }
  }
  return true;
}

auto resolve_cgroup2_path(const cgroup2_mount& mount,
                          const std::filesystem::path& path)
  -> std::filesystem::path {
  auto absolute_path = (std::filesystem::path{"/"} / path).lexically_normal();
  auto root = mount.root.lexically_normal();
  if (root.empty()) {
    root = "/";
  }
  auto relative_path = std::filesystem::path{};
  if (root == "/") {
    relative_path = absolute_path.relative_path();
  } else if (absolute_path == root) {
    relative_path = std::filesystem::path{};
  } else if (auto relative_to_root = absolute_path.lexically_relative(root);
             not relative_to_root.empty()
             and path_stays_below_root(relative_to_root)) {
    relative_path = relative_to_root;
  } else {
    relative_path = path;
  }
  return (mount.mount_point / relative_path).lexically_normal();
}

auto cgroup2_memory_available(const std::filesystem::path& path)
  -> Option<available_memory_info> {
  auto mount = find_cgroup2_mount().value_or(cgroup2_mount{
    .root = "/",
    .mount_point = "/sys/fs/cgroup",
  });
  auto current = resolve_cgroup2_path(mount, path);
  auto root = mount.mount_point.lexically_normal();
  auto result = Option<uint64_t>{};
  while (true) {
    if (auto available = read_cgroup_memory_available(current)) {
      if (not result or *available < *result) {
        result = available;
      }
    }
    if (current == root or current == current.parent_path()) {
      break;
    }
    current = current.parent_path();
  }
  if (not result) {
    return None{};
  }
  return available_memory_info{
    .bytes = *result,
    .source = "cgroup-v2",
  };
}

#endif

auto cgroup_memory_available() -> Option<available_memory_info> {
#if TENZIR_LINUX
  try {
    for (const auto& cgroup : pfs::procfs{}.get_task().get_cgroups()) {
      const auto path = relative_cgroup_path(cgroup.pathname);
      if (cgroup.controllers.empty()) {
        if (auto result = cgroup2_memory_available(path)) {
          return result;
        }
      } else if (std::find(cgroup.controllers.begin(), cgroup.controllers.end(),
                           "memory")
                 != cgroup.controllers.end()) {
        for (auto const& dir : {std::filesystem::path{"/sys/fs/cgroup/memory"},
                                std::filesystem::path{"/sys/fs/cgroup"}}) {
          if (auto result = read_cgroup_memory_available(dir / path)) {
            return available_memory_info{
              .bytes = *result,
              .source = "cgroup-v1",
            };
          }
        }
      }
    }
  } catch (const std::exception&) {
  }
#endif
  if (auto result = read_cgroup_memory_available("/sys/fs/cgroup")) {
    return available_memory_info{
      .bytes = *result,
      .source = "cgroup",
    };
  }
  return None{};
}

auto procfs_memory_available() -> Option<available_memory_info> {
  auto meminfo = std::ifstream{"/proc/meminfo"};
  auto key = std::string{};
  auto value = uint64_t{};
  auto unit = std::string{};
  while (meminfo >> key >> value >> unit) {
    if (key == "MemAvailable:") {
      return available_memory_info{
        .bytes = value * uint64_t{1024},
        .source = "/proc/meminfo",
      };
    }
  }
  return None{};
}

auto mach_memory_available() -> Option<available_memory_info> {
#if defined(__APPLE__) && __has_include(<mach/mach.h>)
  static const auto page_size = static_cast<uint64_t>(getpagesize());
  auto vm_count = mach_msg_type_number_t{HOST_VM_INFO64_COUNT};
  auto vm = vm_statistics64_data_t{};
  if (KERN_SUCCESS
      != host_statistics64(mach_host_self(), HOST_VM_INFO64,
                           reinterpret_cast<host_info64_t>(&vm), &vm_count)) {
    return None{};
  }
  return available_memory_info{
    .bytes
    = static_cast<uint64_t>(vm.free_count + vm.inactive_count) * page_size,
    .source = "mach",
  };
#else
  return None{};
#endif
}

} // namespace

auto available_memory() -> Option<available_memory_info> {
  auto cgroup = cgroup_memory_available();
  auto system = procfs_memory_available();
  if (not system) {
    system = mach_memory_available();
  }
  if (cgroup and system and system->bytes < cgroup->bytes) {
    return system;
  }
  if (cgroup) {
    return cgroup;
  }
  return system;
}

} // namespace tenzir::detail
