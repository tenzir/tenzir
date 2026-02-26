//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <string_view>
#include <vector>

namespace tenzir {

enum class MetricsDirection : std::uint8_t { read, write };
enum class MetricsVisibility : std::uint8_t { external_, internal_ };
enum class MetricsType : std::uint8_t { counter, gauge };

/// A (key, value) label attached to a counter.
/// Key and value are bounded to `max_length` characters.
class MetricsLabel {
public:
  static constexpr std::size_t max_length = 16;

  struct FixedString {
    std::array<char, max_length> data_{};
    FixedString() = default;
    template <std::size_t N>
      requires(N <= max_length)
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays)
    constexpr FixedString(char const (&str)[N]) {
      std::copy_n(str, N, data_.begin());
    }

    static auto truncate(std::string_view sv) -> FixedString {
      auto result = FixedString{};
      auto n = std::min(sv.size(), max_length - 1);
      std::copy_n(sv.data(), n, result.data_.begin());
      return result;
    }

    auto view() const noexcept -> std::string_view {
      if (data_.back() == '\0') {
        return std::string_view{data_.data()};
      }
      return std::string_view{data_.data(), data_.size()};
    }
  };

  template <std::size_t KeyN, std::size_t ValueN>
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays)
  constexpr MetricsLabel(char const (&key)[KeyN], char const (&value)[ValueN])
    : key_{key}, value_{value} {
  }
  MetricsLabel(FixedString key, FixedString value) : key_{key}, value_{value} {
  }
  auto key() const noexcept -> std::string_view {
    return key_.view();
  }
  auto value() const noexcept -> std::string_view {
    return value_.view();
  }

private:
  FixedString key_;
  FixedString value_;
};

template <MetricsType Type>
class Metric {
public:
  constexpr static auto type = Type;
  /// Constructs a null counter where `add()` is a no-op.
  Metric() = default;

  void add(uint64_t bytes);
  void remove(uint64_t bytes)
    requires(Type == MetricsType::gauge);
  void set(uint64_t bytes)
    requires(Type == MetricsType::gauge);

  explicit operator bool() const;

private:
  friend class PipelineMetrics;

  explicit Metric(std::shared_ptr<std::atomic<uint64_t>> value);

  std::shared_ptr<std::atomic<uint64_t>> value_;
};

using MetricsCounter = Metric<MetricsType::counter>;
using MetricsGauge = Metric<MetricsType::gauge>;

/// Snapshot of a single counter (plain values, no atomics).
struct MetricsSnapshotEntry {
  MetricsLabel label;
  MetricsDirection direction = {};
  MetricsVisibility visibility = {};
  MetricsType type = {};
  uint64_t value = {};
};

/// Callback type for periodic emission.
using MetricsCallback
  = std::function<void(std::span<const MetricsSnapshotEntry>)>;

/// Per-pipeline collection of labeled counters.
///
/// Thread-safe: `make_counter()` can be called from operator coroutines,
/// `take_snapshot()` from the timer coroutine.
class PipelineMetrics {
public:
  template <MetricsType Type>
  auto make(MetricsLabel label, MetricsDirection direction,
            MetricsVisibility visibility) -> Metric<Type>;

  /// Create and register a new counter.
  auto make_counter(MetricsLabel label, MetricsDirection direction,
                    MetricsVisibility visibility) -> MetricsCounter {
    return make<MetricsType::counter>(label, direction, visibility);
  }

  /// Create and register a new counter.
  auto make_gauge(MetricsLabel label, MetricsDirection direction,
                  MetricsVisibility visibility) -> MetricsGauge {
    return make<MetricsType::gauge>(label, direction, visibility);
  }

  /// Read all counters into plain snapshots.
  auto take_snapshot() -> std::vector<MetricsSnapshotEntry>;

private:
  struct entry {
    MetricsLabel label;
    MetricsDirection direction;
    MetricsVisibility visibility;
    MetricsType type;
    std::shared_ptr<std::atomic<uint64_t>> value;

    auto snapshot() const -> MetricsSnapshotEntry;
  };

  std::mutex mutex_;
  std::vector<entry> entries_;
};

} // namespace tenzir
