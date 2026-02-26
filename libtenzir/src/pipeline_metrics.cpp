//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_metrics.hpp"

namespace tenzir {

template <MetricsType Type>
Metric<Type>::Metric(std::shared_ptr<std::atomic<uint64_t>> value)
  : value_{std::move(value)} {
}

template <MetricsType type>
void Metric<type>::add(uint64_t bytes) {
  if (value_) {
    value_->fetch_add(bytes, std::memory_order_relaxed);
  }
}

template <MetricsType Type>
void Metric<Type>::remove(uint64_t bytes)
  requires(Type == MetricsType::gauge)
{
  if (value_) {
    value_->fetch_sub(bytes, std::memory_order_relaxed);
  }
}

template <MetricsType Type>
void Metric<Type>::set(uint64_t bytes)
  requires(Type == MetricsType::gauge)
{
  if (value_) {
    value_->store(bytes, std::memory_order_relaxed);
  }
}

template <MetricsType type>
Metric<type>::operator bool() const {
  return value_ != nullptr;
}

template class Metric<MetricsType::counter>;

template class Metric<MetricsType::gauge>;

template <MetricsType Type>
auto PipelineMetrics::make(MetricsLabel label, MetricsDirection direction,
                           MetricsVisibility visibility) -> Metric<Type> {
  auto value = std::make_shared<std::atomic<uint64_t>>(0);
  auto lock = std::lock_guard{mutex_};
  entries_.push_back(entry{
    .label = label,
    .direction = direction,
    .visibility = visibility,
    .type = Type,
    .value = value,
  });
  return Metric<Type>{std::move(value)};
}

template auto
  PipelineMetrics::make<MetricsType::counter>(MetricsLabel, MetricsDirection,
                                              MetricsVisibility)
    -> Metric<MetricsType::counter>;

template auto
  PipelineMetrics::make<MetricsType::gauge>(MetricsLabel, MetricsDirection,
                                            MetricsVisibility)
    -> Metric<MetricsType::gauge>;

auto PipelineMetrics::entry::snapshot() const -> MetricsSnapshotEntry {
  return MetricsSnapshotEntry{
    .label = label,
    .direction = direction,
    .visibility = visibility,
    .type = type,
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
