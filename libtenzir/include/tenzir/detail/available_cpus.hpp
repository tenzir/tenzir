//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"

#include <cstddef>
#include <filesystem>

namespace tenzir::detail {

/// The number of CPUs that this process can keep busy at the same time: the
/// CPUs it may run on, capped by the CPU quotas of the cgroups it is in. At
/// least one.
///
/// Unlike `std::thread::hardware_concurrency()`, this respects the CPU limit
/// of a container, which restricts the quota rather than the CPUs the process
/// may run on.
auto available_cpus() -> size_t;

/// The CPU quota of the cgroup whose control files live in `dir`, as a number
/// of CPUs rounded up, or nothing if it has no readable quota. Reads cgroup
/// v2's `cpu.max`, or else cgroup v1's `cpu.cfs_quota_us` and
/// `cpu.cfs_period_us`. Exposed for testing; use `available_cpus()` elsewhere.
auto read_cgroup_cpu_limit(std::filesystem::path const& dir) -> Option<size_t>;

/// The tightest CPU quota of the cgroup at `path` below the hierarchy mounted
/// at `root` and of all its ancestors up to `root`, since each of them limits
/// its whole subtree. Exposed for testing; use `available_cpus()` elsewhere.
auto read_cgroup_cpu_limit(std::filesystem::path const& root,
                           std::filesystem::path const& path) -> Option<size_t>;

} // namespace tenzir::detail
