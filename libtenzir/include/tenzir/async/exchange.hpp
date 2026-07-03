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
#include "tenzir/panic.hpp"
#include "tenzir/routing.hpp"
#include "tenzir/table_slice.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

namespace tenzir {

/// Routing policy for a scatter exchange: distribute stateless work across
/// lanes while balancing load. For events, this splits slices by row using the
/// adaptive water-fill policy; for other element types it round-robins whole
/// messages.
struct RoundRobinAdaptive {};

/// The routing policy of a scatter exchange.
///
/// Hash-partitioned routing is a later phase; only round-robin exists today.
using ScatterPolicy = variant<RoundRobinAdaptive>;

/// The fan-out endpoint of a scatter exchange.
///
/// A `ScatterPush` is held by the single upstream operator and forwards each
/// message to one or more of its `n` downstream lanes:
///
/// - Data is routed by the `ScatterPolicy`. `RoundRobinAdaptive` splits event
///   slices across lanes by row (via `routing::distribute_adaptive`) and
///   round-robins non-event messages.
/// - Signals are broadcast to every open lane, sequentially. Blocking on a slow
///   lane is correct: it applies backpressure.
///
/// The control plane may retire a lane with `close_lane` once its downstream
/// finished (e.g. `head`); the scatter then stops routing data to it while
/// still forwarding signals to the remaining lanes.
template <class T>
class ScatterPush final : public Push<OperatorMsg<T>> {
public:
  ScatterPush(std::vector<Box<Push<OperatorMsg<T>>>> lanes,
              ScatterPolicy policy)
    : lanes_{std::move(lanes)},
      open_(lanes_.size(), true),
      policy_{policy},
      rows_assigned_(lanes_.size(), 0) {
    TENZIR_ASSERT(not lanes_.empty());
  }

  auto operator()(OperatorMsg<T> msg) -> Task<void> override {
    return co_match(
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
    return match(policy_, [&](const RoundRobinAdaptive&) -> Task<void> {
      return route_round_robin(std::move(data));
    });
  }

  auto route_round_robin(T data) -> Task<void> {
    if constexpr (std::same_as<T, table_slice>) {
      // Split the slice across open lanes by row, keeping load balanced.
      auto total = static_cast<uint64_t>(data.rows());
      if (total == 0) {
        co_return;
      }
      // `distribute_adaptive` works over all lanes; funnel closed lanes by
      // pre-loading them so they are never selected.
      auto assignments
        = routing::distribute_adaptive(total, effective_rows_assigned());
      auto offset = size_t{0};
      for (auto [lane, count] : assignments) {
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

  /// For adaptive routing, returns a load vector where closed lanes are pinned
  /// to the maximum so they never receive rows, and records the real assignment
  /// back into `rows_assigned_`.
  auto effective_rows_assigned() -> std::vector<uint64_t>& {
    // Closed lanes are excluded by setting their load extremely high, which
    // keeps `distribute_adaptive` from ever choosing them (it always fills the
    // least-loaded first and checks fairness against the minimum).
    for (auto i = size_t{0}; i < open_.size(); ++i) {
      if (not open_[i]) {
        rows_assigned_[i] = std::numeric_limits<uint64_t>::max();
      }
    }
    return rows_assigned_;
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
  ScatterPolicy policy_;
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
auto make_scatter(size_t lanes, ScatterPolicy policy, Factory make_channel,
                  ChannelId id)
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
    = Box<Push<OperatorMsg<T>>>{ScatterPush<T>{std::move(lane_pushes), policy}};
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
