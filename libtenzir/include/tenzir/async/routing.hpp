//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/executor.hpp"
#include "tenzir/async/push_pull.hpp"
#include "tenzir/async/select_set.hpp"
#include "tenzir/async/signal.hpp"
#include "tenzir/box.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/multi_series.hpp"
#include "tenzir/panic.hpp"
#include "tenzir/table_slice.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

/// Routing policies shared between the explicit `parallel` operator and the
/// implicit parallelization exchanges (scatter/gather).
namespace tenzir::routing {

/// The maximum allowed ratio between the most- and least-loaded worker for a
/// distribution to be considered fair by `distribute_adaptive`.
inline constexpr auto fairness_factor = 2.0;

/// Distributes `total` rows across `k` workers, leveling them up from the
/// least-loaded first. Workers with fewer rows assigned get more rows, bringing
/// everyone as close to equal as possible. Any leftover rows after leveling are
/// split evenly.
///
/// The `sorted_indices` must be sorted by ascending `rows_assigned`.
///
/// Example: rows_assigned = [100, 300, 500], total = 1000
///   Level up worker 0 by 200 to match worker 1 (cost: 200)
///   Level up workers 0,1 by 200 each to match worker 2 (cost: 400)
///   Remaining 400 split evenly: 134, 133, 133
///   Result: [534, 333, 133], new totals: [634, 633, 633]
///
/// The returned vector has size `k` and is indexed by position within
/// `sorted_indices` (not by worker id).
auto water_fill(uint64_t total, std::span<const size_t> sorted_indices,
                std::span<const uint64_t> rows_assigned)
  -> std::vector<uint64_t>;

/// Distributes `total_rows` across workers while maintaining fairness.
///
/// Tries to use as few workers as possible (for better locality) while keeping
/// the max/min ratio of total rows assigned across all workers within
/// `fairness_factor`. Starts by trying to send everything to the most-starved
/// worker (k=1), then considers spreading across 2, 3, ... workers until the
/// fairness constraint is satisfied. At k=n (all workers), always accepts.
///
/// Updates `rows_assigned` in place and returns (worker_index, row_count)
/// pairs. Only workers that receive a non-zero number of rows are included.
///
/// Example: 4 workers at [0, 0, 0, 0], distributing 1000 rows
///   k=1: all to worker 0 → [1000, 0, 0, 0], unfair → rejected
///   k=4: 250 each → [250, 250, 250, 250] → accepted
///
/// Example: 4 workers at [500, 300, 200, 100], distributing 400 rows
///   k=1: all to worker 3 → [500, 300, 200, 500], max/min = 2.5 → rejected
///   k=2: water-fill workers 3,2 → [500, 300, 350, 350], max/min = 1.67 → ok
auto distribute_adaptive(uint64_t total_rows,
                         std::vector<uint64_t>& rows_assigned)
  -> std::vector<std::pair<size_t, uint64_t>>;

/// A contiguous run of rows `[begin, end)` that all hash to the same `bucket`.
struct hash_run {
  uint64_t bucket;
  int64_t begin;
  int64_t end;

  friend auto operator==(const hash_run&, const hash_run&) -> bool = default;
};

/// Splits `values` into maximal contiguous runs of rows that hash to the same
/// bucket, where `bucket = hash(value) % jobs`. Preserves input order: the
/// returned runs partition `[0, values.length())` from front to back.
///
/// This is the routing primitive behind hash-partitioned exchanges: rows within
/// a run can be pushed as a single subslice to the owning lane.
///
/// Requires `jobs > 0`.
auto hash_runs(const multi_series& values, uint64_t jobs)
  -> std::vector<hash_run>;

} // namespace tenzir::routing

namespace tenzir {

/// The fan-out endpoint of a scatter exchange.
///
/// A `ScatterPush` is held by the single upstream operator and forwards each
/// message to one or more of its `n` downstream lanes:
///
/// - Data is routed round-robin with adaptive load balancing: event slices are
///   split across lanes by row (via `routing::distribute_adaptive`) while other
///   element types round-robin whole messages.
/// - Signals are broadcast to every open lane, sequentially. Blocking on a slow
///   lane is correct: it applies backpressure.
///
/// The control plane may retire a lane with `close_lane` once its downstream
/// finished (e.g. `head`); the scatter then stops routing data to it while
/// still forwarding signals to the remaining lanes.
template <class T>
class ScatterPush final : public Push<OperatorMsg<T>> {
public:
  explicit ScatterPush(std::vector<Box<Push<OperatorMsg<T>>>> lanes)
    : lanes_{std::move(lanes)},
      open_(lanes_.size(), true),
      rows_assigned_(lanes_.size(), 0) {
    TENZIR_ASSERT(not lanes_.empty());
  }

  auto operator()(OperatorMsg<T> msg) -> Task<void> override {
    // Note the `co_await`: `operator()` must itself be a coroutine so the
    // handler lambda temporaries created by `co_match` stay alive across the
    // suspension. A plain `return co_match(...)` would destroy them at the end
    // of the full expression, leaving the returned (lazily-started) handler
    // coroutine with a dangling reference to its captured `this`.
    co_await co_match(
      std::move(msg),
      [this](Signal signal) -> Task<void> {
        // Broadcast signals to every open lane, sequentially.
        for (auto i = size_t{0}; i < lanes_.size(); ++i) {
          if (open_[i]) {
            co_await (*lanes_[i])(OperatorMsg<T>{signal});
          }
        }
      },
      [this](T data) -> Task<void> {
        co_await route_data(std::move(data));
      });
  }

  /// Retires a lane so no further data is routed to it. Signals are still
  /// broadcast to open lanes only.
  auto close_lane(size_t lane) -> void {
    TENZIR_ASSERT(lane < open_.size());
    open_[lane] = false;
  }

  /// Returns the number of lanes still receiving data.
  auto open_lanes() const -> size_t {
    return std::ranges::count(open_, true);
  }

private:
  auto route_data(T data) -> Task<void> {
    if constexpr (std::same_as<T, table_slice>) {
      // Split the slice across open lanes by row, keeping load balanced.
      auto total = static_cast<uint64_t>(data.rows());
      if (total == 0) {
        co_return;
      }
      // Distribute across open lanes only. Retired lanes are excluded from the
      // load vector entirely, so they neither receive rows nor skew the
      // fairness check (a pinned sentinel would reject every `k < n`, forcing
      // needless fragmentation across all open lanes).
      auto open_lane_ids = std::vector<size_t>{};
      auto open_loads = std::vector<uint64_t>{};
      for (auto i = size_t{0}; i < open_.size(); ++i) {
        if (open_[i]) {
          open_lane_ids.push_back(i);
          open_loads.push_back(rows_assigned_[i]);
        }
      }
      if (open_lane_ids.empty()) {
        panic("scatter has no open lane to route to");
      }
      // `distribute_adaptive` updates `open_loads` in place for the lanes it
      // fills; fold those back into the real per-lane load vector.
      auto assignments = routing::distribute_adaptive(total, open_loads);
      auto offset = size_t{0};
      for (auto [compact, count] : assignments) {
        auto lane = open_lane_ids[compact];
        rows_assigned_[lane] = open_loads[compact];
        auto slice = subslice(data, offset, offset + count);
        offset += count;
        co_await (*lanes_[lane])(OperatorMsg<table_slice>{std::move(slice)});
      }
    } else {
      // Round-robin whole messages across open lanes.
      auto lane = next_open_lane();
      co_await (*lanes_[lane])(OperatorMsg<T>{std::move(data)});
    }
  }

  auto next_open_lane() -> size_t {
    for (auto step = size_t{0}; step < lanes_.size(); ++step) {
      auto lane = (rr_cursor_ + step) % lanes_.size();
      if (open_[lane]) {
        rr_cursor_ = (lane + 1) % lanes_.size();
        return lane;
      }
    }
    panic("scatter has no open lane to route to");
  }

  std::vector<Box<Push<OperatorMsg<T>>>> lanes_;
  std::vector<bool> open_;
  std::vector<uint64_t> rows_assigned_;
  size_t rr_cursor_ = 0;
};

/// Drives the fan-in of a gather exchange.
///
/// Runs the merge loop that reads the `n` upstream lane pulls and writes the
/// aligned stream into `out`. Intended to be spawned as a background task; when
/// it returns, `out` is dropped and the downstream `Pull` observes closure.
///
/// Per-signal alignment policy:
///
/// - data: forwarded as received (interleaved).
/// - `EndOfData`: counted; emitted once after every lane delivered it.
/// - lane drained (`None`): counted; when all lanes drained the loop stops.
/// - `Checkpoint`: aligned barrier, not yet implemented.
///
/// Pull-based selection makes this cheap: a fast lane naturally stalls under
/// backpressure while a slow lane catches up.
template <class T>
auto run_gather(std::vector<Box<Pull<OperatorMsg<T>>>> lanes,
                Box<Push<OperatorMsg<T>>> out) -> Task<void> {
  struct LaneMsg {
    size_t lane;
    Option<OperatorMsg<T>> msg;
  };
  const auto n = lanes.size();
  TENZIR_ASSERT(n > 0);
  auto eod_count = size_t{0};
  auto drained = size_t{0};
  auto set = SelectSet<LaneMsg>{};
  co_await set.activate([&]() -> Task<void> {
    auto arm = [&](size_t lane) {
      set.add([&lanes, lane]() -> Task<LaneMsg> {
        co_return LaneMsg{lane, co_await (*lanes[lane])()};
      });
    };
    for (auto lane = size_t{0}; lane < n; ++lane) {
      arm(lane);
    }
    while (auto next = co_await set.next([](const LaneMsg&) {
      return true;
    })) {
      auto lane = next->lane;
      if (not next->msg) {
        // The lane drained; do not re-arm it.
        ++drained;
        if (drained == n) {
          break;
        }
        continue;
      }
      auto stop = co_await co_match(
        std::move(*next->msg),
        [&](T data) -> Task<bool> {
          co_await (*out)(OperatorMsg<T>{std::move(data)});
          arm(lane);
          co_return false;
        },
        [&](Signal signal) -> Task<bool> {
          co_return co_await co_match(
            std::move(signal),
            [&](EndOfData) -> Task<bool> {
              // Emit a single aligned end-of-data once all lanes delivered it.
              if (++eod_count == n) {
                co_await (*out)(OperatorMsg<T>{Signal{EndOfData{}}});
                eod_count = 0;
              }
              // Keep polling the lane for its eventual drain.
              arm(lane);
              co_return false;
            },
            [&](Checkpoint) -> Task<bool> {
              // Aligned barrier handling is deferred to the checkpointing epic.
              TENZIR_TODO();
              co_return true;
            });
        });
      if (stop) {
        break;
      }
    }
  });
}

/// Creates a scatter exchange with `lanes` downstream lanes.
///
/// Returns the single upstream `Push` (a `ScatterPush`) and the `lanes` lane
/// `Pull`s. `make_channel` produces one internal SPSC channel per lane, e.g.
/// `ExecCtx::make_channel<T>`.
template <class T, class Factory>
auto make_scatter(size_t lanes, Factory make_channel, ChannelId id)
  -> std::pair<Box<Push<OperatorMsg<T>>>,
               std::vector<Box<Pull<OperatorMsg<T>>>>> {
  TENZIR_ASSERT(lanes > 0);
  auto lane_pushes = std::vector<Box<Push<OperatorMsg<T>>>>{};
  auto lane_pulls = std::vector<Box<Pull<OperatorMsg<T>>>>{};
  lane_pushes.reserve(lanes);
  lane_pulls.reserve(lanes);
  for (auto lane = size_t{0}; lane < lanes; ++lane) {
    auto pair
      = make_channel(ChannelId{fmt::format("{}#scatter/{}", id.value, lane)});
    lane_pushes.push_back(std::move(pair.push));
    lane_pulls.push_back(std::move(pair.pull));
  }
  auto scatter
    = Box<Push<OperatorMsg<T>>>{ScatterPush<T>{std::move(lane_pushes)}};
  return {std::move(scatter), std::move(lane_pulls)};
}

/// The parts of a gather exchange with `lanes` upstream lanes.
template <class T>
struct GatherParts {
  /// One `Push` per lane, held by each upstream lane operator.
  std::vector<Box<Push<OperatorMsg<T>>>> lanes;
  /// The single downstream `Pull`, held by the downstream operator.
  Box<Pull<OperatorMsg<T>>> pull;
  /// The merge loop; the caller must spawn this task.
  Task<void> merger;
};

/// Creates a gather exchange with `lanes` upstream lanes.
///
/// The caller is responsible for spawning `GatherParts::merger` on a suitable
/// scope. `make_channel` produces the internal SPSC channels, e.g.
/// `ExecCtx::make_channel<T>`.
template <class T, class Factory>
auto make_gather(size_t lanes, Factory make_channel, ChannelId id)
  -> GatherParts<T> {
  TENZIR_ASSERT(lanes > 0);
  auto lane_pushes = std::vector<Box<Push<OperatorMsg<T>>>>{};
  auto lane_pulls = std::vector<Box<Pull<OperatorMsg<T>>>>{};
  lane_pushes.reserve(lanes);
  lane_pulls.reserve(lanes);
  for (auto lane = size_t{0}; lane < lanes; ++lane) {
    auto pair
      = make_channel(ChannelId{fmt::format("{}#gather/{}", id.value, lane)});
    lane_pushes.push_back(std::move(pair.push));
    lane_pulls.push_back(std::move(pair.pull));
  }
  auto out = make_channel(ChannelId{fmt::format("{}#gather/out", id.value)});
  return GatherParts<T>{
    .lanes = std::move(lane_pushes),
    .pull = std::move(out.pull),
    .merger = run_gather<T>(std::move(lane_pulls), std::move(out.push)),
  };
}

} // namespace tenzir
