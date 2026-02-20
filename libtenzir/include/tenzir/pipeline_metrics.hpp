//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace tenzir {

enum class metrics_direction { read, write };
enum class metrics_visibility { external_, internal_ };

/// A (key, value) label attached to a counter.
struct metrics_label {
  std::string key;
  std::string value;
};

/// Thread-safe counter. Wraps a `shared_ptr<atomic<uint64_t>>`.
/// Cheap to copy, safe to capture in lambdas and pass to background tasks.
class metrics_counter {
public:
  /// Constructs a null counter where `add()` is a no-op.
  metrics_counter() = default;

  void add(uint64_t bytes);

  explicit operator bool() const;

private:
  friend class pipeline_metrics;

  explicit metrics_counter(std::shared_ptr<std::atomic<uint64_t>> value);

  std::shared_ptr<std::atomic<uint64_t>> value_;
};

/// Snapshot of a single counter (plain values, no atomics).
struct metrics_snapshot_entry {
  metrics_label label;
  metrics_direction direction = {};
  metrics_visibility visibility = {};
  uint64_t value = {};
};

/// Per-pipeline collection of labeled counters.
///
/// Thread-safe: `make_counter()` can be called from operator coroutines,
/// `take_snapshot()` from the timer coroutine.
class pipeline_metrics {
public:
  /// Create and register a new counter.
  auto make_counter(metrics_label label, metrics_direction direction,
                    metrics_visibility visibility) -> metrics_counter;

  /// Read all counters into plain snapshots.
  auto take_snapshot() -> std::vector<metrics_snapshot_entry>;

private:
  struct entry {
    metrics_label label;
    metrics_direction direction;
    metrics_visibility visibility;
    std::shared_ptr<std::atomic<uint64_t>> value;
  };

  std::mutex mutex_;
  std::vector<entry> entries_;
};

} // namespace tenzir
