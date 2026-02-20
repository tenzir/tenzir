//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_metrics.hpp"

namespace tenzir {

MetricsCounter::MetricsCounter(std::shared_ptr<std::atomic<uint64_t>> value)
  : value_{std::move(value)} {
}

void MetricsCounter::add(uint64_t bytes) {
  if (value_) {
    value_->fetch_add(bytes, std::memory_order_relaxed);
  }
}

MetricsCounter::operator bool() const {
  return value_ != nullptr;
}

auto PipelineMetrics::make_counter(MetricsLabel label,
                                   MetricsDirection direction,
                                   MetricsVisibility visibility)
  -> MetricsCounter {
  auto value = std::make_shared<std::atomic<uint64_t>>(0);
  auto lock = std::lock_guard{mutex_};
  entries_.push_back(entry{
    .label = label,
    .direction = direction,
    .visibility = visibility,
    .value = value,
  });
  return MetricsCounter{std::move(value)};
}

auto PipelineMetrics::entry::snapshot() const -> MetricsSnapshotEntry {
  return MetricsSnapshotEntry{
    .label = label,
    .direction = direction,
    .visibility = visibility,
    .value = value->load(std::memory_order_relaxed),
  };
}

auto PipelineMetrics::take_snapshot() -> std::vector<MetricsSnapshotEntry> {
  auto result = std::vector<MetricsSnapshotEntry>{};
  auto lock = std::lock_guard{mutex_};
  result.reserve(entries_.size());
  for (auto const& e : entries_) {
    result.push_back(e.snapshot());
  }
  return result;
}

} // namespace tenzir
