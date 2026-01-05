//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_buffer_stats.hpp"

namespace tenzir {

auto pipeline_buffer_registry::instance() -> pipeline_buffer_registry& {
  static pipeline_buffer_registry registry;
  return registry;
}

auto pipeline_buffer_registry::get_or_create(std::string_view pipeline_id)
  -> std::shared_ptr<pipeline_buffer_stats> {
  auto lock = std::lock_guard{mutex_};
  auto it = stats_.find(pipeline_id);
  if (it != stats_.end()) {
    if (auto ptr = it->second.lock()) {
      return ptr;
    }
    // Weak pointer expired, will be replaced below
  }
  auto stats = std::make_shared<pipeline_buffer_stats>();
  stats_.insert_or_assign(std::string{pipeline_id}, stats);
  return stats;
}

auto pipeline_buffer_registry::snapshot()
  -> std::vector<std::tuple<std::string, uint64_t, uint64_t>> {
  auto result = std::vector<std::tuple<std::string, uint64_t, uint64_t>>{};
  auto expired = std::vector<std::string>{};
  {
    auto lock = std::lock_guard{mutex_};
    for (const auto& [id, weak] : stats_) {
      if (auto ptr = weak.lock()) {
        result.emplace_back(id, ptr->bytes.load(std::memory_order_relaxed),
                            ptr->events.load(std::memory_order_relaxed));
      } else {
        expired.push_back(id);
      }
    }
    // Clean up expired entries
    for (const auto& id : expired) {
      stats_.erase(id);
    }
  }
  return result;
}

} // namespace tenzir
