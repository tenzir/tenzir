//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_metrics.hpp"

namespace tenzir {

metrics_counter::metrics_counter(std::shared_ptr<std::atomic<uint64_t>> value)
  : value_{std::move(value)} {
}

void metrics_counter::add(uint64_t bytes) {
  if (value_) {
    value_->fetch_add(bytes, std::memory_order_relaxed);
  }
}

metrics_counter::operator bool() const {
  return value_ != nullptr;
}

auto pipeline_metrics::make_counter(metrics_label label,
                                    metrics_direction direction,
                                    metrics_visibility visibility)
  -> metrics_counter {
  auto value = std::make_shared<std::atomic<uint64_t>>(0);
  auto lock = std::lock_guard{mutex_};
  entries_.push_back(entry{
    .label = std::move(label),
    .direction = direction,
    .visibility = visibility,
    .value = value,
  });
  return metrics_counter{std::move(value)};
}

auto pipeline_metrics::take_snapshot() -> std::vector<metrics_snapshot_entry> {
  auto result = std::vector<metrics_snapshot_entry>{};
  auto lock = std::lock_guard{mutex_};
  result.reserve(entries_.size());
  for (auto const& e : entries_) {
    result.push_back(metrics_snapshot_entry{
      .label = e.label,
      .direction = e.direction,
      .visibility = e.visibility,
      .value = e.value->load(std::memory_order_relaxed),
    });
  }
  return result;
}

} // namespace tenzir
