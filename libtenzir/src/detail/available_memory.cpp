//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/available_memory.hpp"

#include "tenzir/config.hpp"

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

namespace tenzir::detail {

namespace {

auto read_memory_value(const std::filesystem::path& path)
  -> std::optional<uint64_t> {
  auto file = std::ifstream{path};
  auto value = std::string{};
  file >> value;
  if (value.empty() or value == "max") {
    return std::nullopt;
  }
  try {
    return std::stoull(value);
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

auto read_cgroup_memory_available(const std::filesystem::path& dir,
                                  std::string source)
  -> std::optional<available_memory_info> {
  auto current = read_memory_value(dir / "memory.current");
  auto max = read_memory_value(dir / "memory.max");
  if (not current or not max) {
    current = read_memory_value(dir / "memory.usage_in_bytes");
    max = read_memory_value(dir / "memory.limit_in_bytes");
  }
  if (not current or not max) {
    return std::nullopt;
  }
  static constexpr auto unlimited_cgroup_limit = uint64_t{1} << 60;
  if (*max >= unlimited_cgroup_limit) {
    return std::nullopt;
  }
  if (*current >= *max) {
    return available_memory_info{
      .bytes = 0,
      .source = std::move(source),
    };
  }
  return available_memory_info{
    .bytes = *max - *current,
    .source = std::move(source),
  };
}

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

auto find_cgroup2_mount() -> std::optional<cgroup2_mount> {
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
  return std::nullopt;
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
  -> std::optional<available_memory_info> {
  auto mount = find_cgroup2_mount().value_or(cgroup2_mount{
    .root = "/",
    .mount_point = "/sys/fs/cgroup",
  });
  auto current = resolve_cgroup2_path(mount, path);
  auto root = mount.mount_point.lexically_normal();
  auto result = std::optional<available_memory_info>{};
  while (true) {
    if (auto available = read_cgroup_memory_available(current, "cgroup-v2")) {
      if (not result or available->bytes < result->bytes) {
        result = std::move(available);
      }
    }
    if (current == root or current == current.parent_path()) {
      break;
    }
    current = current.parent_path();
  }
  return result;
}

auto cgroup_memory_available() -> std::optional<available_memory_info> {
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
        if (auto result = read_cgroup_memory_available(
              std::filesystem::path{"/sys/fs/cgroup/memory"} / path,
              "cgroup-v1")) {
          return result;
        }
        if (auto result = read_cgroup_memory_available(
              std::filesystem::path{"/sys/fs/cgroup"} / path, "cgroup-v1")) {
          return result;
        }
      }
    }
  } catch (const std::exception&) {
  }
#endif
  return read_cgroup_memory_available("/sys/fs/cgroup", "cgroup");
}

auto procfs_memory_available() -> std::optional<available_memory_info> {
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
  return std::nullopt;
}

auto mach_memory_available() -> std::optional<available_memory_info> {
#if defined(__APPLE__) && __has_include(<mach/mach.h>)
  static const auto page_size = static_cast<uint64_t>(getpagesize());
  auto vm_count = mach_msg_type_number_t{HOST_VM_INFO64_COUNT};
  auto vm = vm_statistics64_data_t{};
  if (KERN_SUCCESS
      != host_statistics64(mach_host_self(), HOST_VM_INFO64,
                           reinterpret_cast<host_info64_t>(&vm), &vm_count)) {
    return std::nullopt;
  }
  return available_memory_info{
    .bytes
    = static_cast<uint64_t>(vm.free_count + vm.inactive_count) * page_size,
    .source = "mach",
  };
#else
  return std::nullopt;
#endif
}

} // namespace

auto available_memory() -> std::optional<available_memory_info> {
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
