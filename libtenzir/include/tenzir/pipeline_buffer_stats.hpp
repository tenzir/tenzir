//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/heterogeneous_string_hash.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

namespace tenzir {

/// Statistics for buffered data in a pipeline's execution nodes.
struct pipeline_buffer_stats {
  std::atomic<uint64_t> bytes{0};
  std::atomic<uint64_t> events{0}; // table_slice only
};

/// Global registry for pipeline buffer statistics.
/// Uses weak_ptr to allow automatic cleanup when all exec nodes are destroyed.
class pipeline_buffer_registry {
public:
  /// Returns the singleton instance.
  static auto instance() -> pipeline_buffer_registry&;

  /// Get or create stats for a pipeline. The returned shared_ptr keeps the
  /// stats alive as long as any exec node holds a reference.
  auto get_or_create(std::string_view pipeline_id)
    -> std::shared_ptr<pipeline_buffer_stats>;

  /// Returns a snapshot of all active pipelines with their current stats.
  /// Cleans up expired weak_ptrs during iteration.
  /// @returns Vector of (pipeline_id, bytes, events).
  auto snapshot() -> std::vector<std::tuple<std::string, uint64_t, uint64_t>>;

private:
  mutable std::mutex mutex_;
  detail::heterogeneous_string_hashmap<std::weak_ptr<pipeline_buffer_stats>>
    stats_;
};

} // namespace tenzir
