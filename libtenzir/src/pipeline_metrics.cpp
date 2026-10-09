//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline_metrics.hpp"

namespace tenzir {

template <MetricsInstrument Instrument>
Metric<Instrument>::Metric(Arc<Atomic<uint64_t>> value)
  : value_{std::move(value)} {
}

template <MetricsInstrument Instrument>
void Metric<Instrument>::add(uint64_t value) {
  if (value_) {
    (*value_)->fetch_add(value, std::memory_order_relaxed);
  }
}

template <MetricsInstrument Instrument>
void Metric<Instrument>::remove(uint64_t value)
  requires(Instrument == MetricsInstrument::gauge)
{
  if (value_) {
    (*value_)->fetch_sub(value, std::memory_order_relaxed);
  }
}

template <MetricsInstrument Instrument>
void Metric<Instrument>::set(uint64_t value)
  requires(Instrument == MetricsInstrument::gauge)
{
  if (value_) {
    (*value_)->store(value, std::memory_order_relaxed);
  }
}

template <MetricsInstrument Instrument>
Metric<Instrument>::operator bool() const {
  return value_.is_some();
}

template class Metric<MetricsInstrument::counter>;

template class Metric<MetricsInstrument::gauge>;

template <MetricsInstrument Instrument>
auto PipelineMetrics::make(MetricsLabel label, MetricsDirection direction,
                           MetricsVisibility visibility, MetricsUnit type,
                           std::string_view connector) -> Metric<Instrument> {
  auto lock = std::lock_guard{mutex_};
  for (auto& e : entries_) {
    // Peer/host labels do not create separate counters. Keep one entry per
    // connector and traffic dimension, preserving legacy unattributed totals.
    if (e.connector == connector and e.direction == direction
        and e.visibility == visibility and e.type == type) {
      return Metric<Instrument>{e.value};
    }
  }
  auto value = Arc<Atomic<uint64_t>>{std::in_place, uint64_t{0}};
  entries_.push_back(Entry{
    .label = label,
    .connector = std::string{connector},
    .direction = direction,
    .visibility = visibility,
    .instrument = Instrument,
    .type = type,
    .value = value,
  });
  return Metric<Instrument>{std::move(value)};
}

template auto PipelineMetrics::make<MetricsInstrument::counter>(
  MetricsLabel, MetricsDirection, MetricsVisibility, MetricsUnit,
  std::string_view) -> Metric<MetricsInstrument::counter>;

template auto
  PipelineMetrics::make<MetricsInstrument::gauge>(MetricsLabel,
                                                  MetricsDirection,
                                                  MetricsVisibility,
                                                  MetricsUnit, std::string_view)
    -> Metric<MetricsInstrument::gauge>;

auto PipelineMetrics::Entry::snapshot() const -> MetricsSnapshotEntry {
  return MetricsSnapshotEntry{
    .label = label,
    .connector = connector,
    .direction = direction,
    .visibility = visibility,
    .instrument = instrument,
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
