//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// @file profiler_snapshot.hpp
/// @brief Aggregation of raw channel and executor counters into the
/// per-operator profile that backs `tenzir.metrics.operator_profile`.

#pragma once

#include "tenzir/arc.hpp"
#include "tenzir/element_type.hpp"
#include "tenzir/operator_id.hpp"
#include "tenzir/time.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <new>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tenzir {

/// Monotonic counters for profiling channel throughput.
struct ChannelStats {
  /// We group/align by in and out here, because that is the grouping in which
  /// these are written.
  struct alignas(std::hardware_destructive_interference_size) data {
    std::atomic<size_t> bytes{0};
    std::atomic<size_t> signals{0};
    std::atomic<size_t> batches{0};
    std::atomic<size_t> events{0};
  };
  data in;
  data out;

  /// Total capacity in bytes across all channels sharing these stats.
  std::atomic<size_t> capacity{0};

  /// Backpressure intervals recorded by the sender.
  struct BackpressureEvent {
    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point end;
  };

  bool record_backpressure = false;

  /// Record a backpressure event (thread-safe). No-op unless
  /// `record_backpressure` is set.
  void add_backpressure(std::chrono::steady_clock::time_point start,
                        std::chrono::steady_clock::time_point end) {
    if (not record_backpressure) {
      return;
    }
    auto lock = std::scoped_lock{bp_mutex_};
    backpressure_events_.push_back({start, end});
  }

  /// Return and clear all backpressure events under lock (thread-safe).
  auto drain_backpressure_events() -> std::vector<BackpressureEvent> {
    auto lock = std::scoped_lock{bp_mutex_};
    return std::exchange(backpressure_events_, {});
  }

private:
  mutable std::mutex bp_mutex_;
  std::vector<BackpressureEvent> backpressure_events_;
};

/// Monotonic counters for profiling per-operator CPU usage.
struct ExecutorStats {
  std::atomic<int64_t> wall_ns{0};
  std::atomic<int64_t> cpu_ns{0};
  std::atomic<size_t> task_count{0};
};

/// Collected profile for a single channel.
struct ChannelProfile {
  ChannelId id;
  Arc<ChannelStats> stats;
  element_type_tag type;
};

/// Collected profile for a single operator's executor.
struct ExecutorProfile {
  OpId id;
  Arc<ExecutorStats> stats;
  std::string name;
};

/// Per-operator snapshot of cumulative counters from the previous tick,
/// used to compute deltas.
struct OpSnapshot {
  size_t bytes_in = 0;
  size_t bytes_out = 0;
  size_t batches_in = 0;
  size_t batches_out = 0;
  size_t events_in = 0;
  size_t events_out = 0;
  size_t signals_in = 0;
  size_t signals_out = 0;
  int64_t cpu_ns = 0;
  int64_t wall_ns = 0;
  size_t task_count = 0;
};

/// Per-operator aggregated profiling data emitted each tick.
struct OperatorProfileEntry {
  std::string operator_id;
  std::string name;
  uint64_t input_bytes = 0;
  uint64_t input_capacity = 0;
  double cpu = 0.0;
  uint64_t task_count = 0;
  uint64_t bytes_in = 0;
  uint64_t bytes_out = 0;
  uint64_t batches_in = 0;
  uint64_t batches_out = 0;
  uint64_t events_in = 0;
  uint64_t events_out = 0;
  uint64_t signals_in = 0;
  uint64_t signals_out = 0;
};

/// Aggregated profiler snapshot emitted each tick.
struct ProfilerSnapshot {
  time timestamp = {};
  /// The time span the counters of this snapshot cover, ending at `timestamp`.
  /// The counters are deltas over this window, so a consumer needs it to derive
  /// a rate. It is measured rather than assumed, and consequently not exactly
  /// the nominal sampling interval.
  tenzir::duration duration = {};
  std::vector<OperatorProfileEntry> operators;
};

/// Aggregate channel and executor profiles into a `ProfilerSnapshot`.
///
/// Computes deltas against `prev` and updates it with the current values.
/// Entries in `prev` for operators that are no longer reported are removed.
///
/// `elapsed` is the time that actually passed since the snapshot in `prev` was
/// taken, measured on a steady clock. It is the divisor for `cpu`, so it must
/// not be the nominal sampling interval: a late tick covers more than one
/// interval and would otherwise inflate the reported usage. A non-positive
/// `elapsed` yields a `cpu` of zero, which is what the very first snapshot of a
/// pipeline reports.
auto build_profiler_snapshot(std::span<ChannelProfile const> channel_profiles,
                             std::span<ExecutorProfile const> executor_profiles,
                             time timestamp, duration elapsed,
                             std::unordered_map<OpId, OpSnapshot>& prev)
  -> ProfilerSnapshot;

} // namespace tenzir
