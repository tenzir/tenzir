//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/available_cpus.hpp"

#include "tenzir/config.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/string.hpp"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <thread>

#if TENZIR_LINUX
#  include <sched.h>
#endif

namespace tenzir::detail {

namespace {

auto read_line(std::filesystem::path const& path) -> Option<std::string> {
  auto file = std::ifstream{path};
  auto result = std::string{};
  if (not std::getline(file, result)) {
    return None{};
  }
  return result;
}

auto parse_integer(std::string_view text) -> Option<int64_t> {
  text = trim(text);
  auto result = int64_t{};
  auto* end = text.data() + text.size();
  auto [ptr, ec] = std::from_chars(text.data(), end, result);
  if (ec != std::errc{} or ptr != end) {
    return None{};
  }
  return result;
}

/// A quota of CPU time per period as a number of CPUs, rounded up so that a
/// fractional CPU still counts. A non-positive quota means no limit.
auto cpus_of(Option<int64_t> quota, Option<int64_t> period) -> Option<size_t> {
  if (not quota or not period or *quota <= 0 or *period <= 0) {
    return None{};
  }
  return narrow<size_t>(*quota / *period + (*quota % *period == 0 ? 0 : 1));
}

} // namespace

auto read_cgroup_cpu_limit(std::filesystem::path const& dir) -> Option<size_t> {
  // cgroup v2 writes `<quota> <period>`, with `max` for no quota.
  if (auto line = read_line(dir / "cpu.max")) {
    auto fields = split(trim(*line), " ");
    if (fields.size() != 2 or fields[0] == "max") {
      return None{};
    }
    return cpus_of(parse_integer(fields[0]), parse_integer(fields[1]));
  }
  // cgroup v1 writes a quota of -1 for no quota.
  auto quota = read_line(dir / "cpu.cfs_quota_us");
  auto period = read_line(dir / "cpu.cfs_period_us");
  if (not quota or not period) {
    return None{};
  }
  return cpus_of(parse_integer(*quota), parse_integer(*period));
}

auto read_cgroup_cpu_limit(std::filesystem::path const& root,
                           std::filesystem::path const& path)
  -> Option<size_t> {
  auto result = read_cgroup_cpu_limit(root);
  auto dir = root;
  // Not normalized, which would turn the `/..` of a path that leaves the
  // hierarchy into `/`, and so read an unrelated cgroup.
  for (auto const& component : path.relative_path()) {
    // A path that leaves the hierarchy, as it does when the process sits in
    // another cgroup namespace, has nothing more to read.
    if (component == "..") {
      break;
    }
    if (component.empty() or component == ".") {
      continue;
    }
    dir /= component;
    if (auto limit = read_cgroup_cpu_limit(dir);
        limit and (not result or *limit < *result)) {
      result = limit;
    }
  }
  return result;
}

auto read_cgroup2_cpu_limit(cgroup2_mount const& mount,
                            std::filesystem::path const& path)
  -> Option<size_t> {
  // Outside of the root, the relative path starts with `..`, where the walk
  // stops.
  return read_cgroup_cpu_limit(mount.mount_point,
                               path.lexically_relative(mount.root));
}

auto available_cpus() -> size_t {
  // Called when a pipeline compiles, and cheap enough for that: it reads a few
  // tiny files from procfs and sysfs.
  auto result = size_t{std::thread::hardware_concurrency()};
#if TENZIR_LINUX
  auto set = cpu_set_t{};
  if (sched_getaffinity(0, sizeof(set), &set) == 0) {
    result = narrow<size_t>(CPU_COUNT(&set));
  }
  // Every line of `/proc/self/cgroup` names the cgroup of one hierarchy as
  // `<id>:<controllers>:<path>`. cgroup v2 has a single one with id 0 and no
  // controllers. A cgroup that is no directory below the mount point, as in
  // another cgroup namespace, leaves the limits of the mount point itself,
  // which then is the cgroup of the container.
  auto cgroups = std::ifstream{"/proc/self/cgroup"};
  auto line = std::string{};
  while (std::getline(cgroups, line)) {
    auto fields = split(line, ":", 2);
    if (fields.size() != 3) {
      continue;
    }
    auto path = std::filesystem::path{std::string{fields[2]}};
    auto limit = Option<size_t>{};
    if (fields[0] == "0" and fields[1].empty()) {
      auto mount = find_cgroup2_mount().value_or(cgroup2_mount{
        .root = "/",
        .mount_point = "/sys/fs/cgroup",
      });
      limit = read_cgroup2_cpu_limit(mount, path);
    } else if (std::ranges::contains(split(fields[1], ","), "cpu")) {
      for (auto const* root :
           {"/sys/fs/cgroup/cpu,cpuacct", "/sys/fs/cgroup/cpuacct,cpu",
            "/sys/fs/cgroup/cpu"}) {
        if (auto found = read_cgroup_cpu_limit(root, path)) {
          limit = found;
          break;
        }
      }
    }
    if (limit) {
      // Zero means the number of CPUs is unknown.
      result = result == 0 ? *limit : std::min(result, *limit);
    }
  }
#endif
  return std::max(result, size_t{1});
}

} // namespace tenzir::detail
