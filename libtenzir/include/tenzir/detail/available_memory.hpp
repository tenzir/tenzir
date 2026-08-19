//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/option.hpp"

#include <cstdint>
#include <filesystem>
#include <string>

namespace tenzir::detail {

struct available_memory_info {
  uint64_t bytes = 0;
  std::string source = {};
};

auto available_memory() -> Option<available_memory_info>;

/// Computes how much memory the cgroup whose control files live in `dir` may
/// still charge, or nothing if `dir` has no readable memory limit. Reclaimable
/// page cache and reclaimable slab count as available rather than as consumed,
/// because the kernel hands those bytes back on demand; a process that merely
/// reads files through `mmap` would otherwise appear to have exhausted the
/// cgroup. Exposed for testing; use `available_memory()` elsewhere.
auto read_cgroup_memory_available(std::filesystem::path const& dir)
  -> Option<uint64_t>;

} // namespace tenzir::detail
