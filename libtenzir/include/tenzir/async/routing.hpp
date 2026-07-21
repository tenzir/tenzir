//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/executor.hpp"
#include "tenzir/async/fused.hpp"
#include "tenzir/async/push_pull.hpp"
#include "tenzir/async/signal.hpp"
#include "tenzir/box.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/multi_series.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>

/// Routing policies shared between the explicit `parallel` operator and the
/// implicit parallelization channels.
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

/// Shared fan-out base for the exchange push endpoints.
///
/// Implements the common message loop: signals are broadcast to every lane
/// sequentially (blocking on a slow lane is correct—it applies backpressure),
/// and non-empty data slices are forwarded to the derived `route_data` policy.
class ExchangePush : public Push<OperatorMsg<table_slice>> {
public:
  auto operator()(OperatorMsg<table_slice> msg) -> Task<void> final;

protected:
  explicit ExchangePush(std::vector<Box<Push<OperatorMsg<table_slice>>>> lanes);

  /// Routes a data slice across `lanes_`.
  virtual auto route_data(table_slice data) -> Task<void> = 0;

  std::vector<Box<Push<OperatorMsg<table_slice>>>> lanes_;
};

/// The fan-out endpoint of a scatter exchange.
///
/// A `ScatterPush` is held by the single upstream operator and forwards each
/// `table_slice` to one or more of its `n` downstream lanes. Data is split
/// across lanes by row via `routing::distribute_adaptive`, keeping total rows
/// assigned as balanced as possible.
class ScatterPush final : public ExchangePush {
public:
  explicit ScatterPush(std::vector<Box<Push<OperatorMsg<table_slice>>>> lanes);

private:
  auto route_data(table_slice data) -> Task<void> override;

  std::vector<uint64_t> rows_assigned_;
};

/// The fan-out endpoint of a broadcast exchange.
///
/// A `BroadcastPush` is held by the single upstream operator and broadcasts
/// each message to its `n` downstream lanes. Unlike `ScatterPush`, which
/// partitions rows so each row lands on exactly one lane, a broadcast sends a
/// copy of the *whole* slice to every lane. `table_slice` copies are cheap
/// (ref-counted Arrow buffers).
class BroadcastPush final : public ExchangePush {
public:
  explicit BroadcastPush(std::vector<Box<Push<OperatorMsg<table_slice>>>> lanes);

private:
  auto route_data(table_slice data) -> Task<void> override;
};

/// The fan-out endpoint of a shuffle exchange.
///
/// A `ShufflePush` is held by a single upstream instance and hash-partitions
/// each `table_slice` across its `n` downstream lanes based on a set of
/// partition-key expressions. The keys are evaluated once per incoming slice,
/// producing a `multi_series`. `routing::hash_runs` splits the slice into
/// maximal contiguous runs of rows that map to the same bucket, and each run
/// is forwarded as a subslice to `lanes_[bucket]`.
class ShufflePush final : public ExchangePush {
public:
  /// Construct a shuffle fan-out.
  ///
  /// If `keys` has more than one element, the entries are wrapped into a
  /// single `ast::record` expression so that a single evaluation per slice
  /// yields a composite key. `dh` must outlive the `ShufflePush`.
  ShufflePush(std::vector<Box<Push<OperatorMsg<table_slice>>>> lanes,
              std::vector<ast::expression> keys, diagnostic_handler& dh);

private:
  auto route_data(table_slice data) -> Task<void> override;

  ast::expression key_;
  diagnostic_handler* dh_;
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
auto run_gather(std::vector<Box<Pull<OperatorMsg<table_slice>>>> lanes,
                Box<Push<OperatorMsg<table_slice>>> out) -> Task<void>;

/// Forwards a single typed `main` stream to `out` while draining `aux` void
/// lanes (the outputs of side-effect sinks), holding the output's completion
/// back until every lane finished.
///
/// Per-signal policy:
///
/// - data: forwarded from `main` as received.
/// - `EndOfData` (only on a non-void `main`): a single aligned end-of-data is
///   emitted downstream once `main` ended and all `aux` lanes drained.
/// - lane drained (`None`): counted; when all lanes drained the loop stops and
///   `out` is dropped, so the downstream observes closure only after every
///   side-effect sink completed.
/// - `Checkpoint`: aligned barrier, not yet implemented.
///
/// `aux` lanes are always `void`: a void channel never carries data or
/// `EndOfData`, so an aux lane signals completion purely by draining.
template <class T>
auto run_gather_signals(Box<Pull<OperatorMsg<T>>> main,
                        std::vector<Box<Pull<OperatorMsg<void>>>> aux,
                        Box<Push<OperatorMsg<T>>> out) -> Task<void>;

extern template auto
  run_gather_signals(Box<Pull<OperatorMsg<void>>>,
                     std::vector<Box<Pull<OperatorMsg<void>>>>,
                     Box<Push<OperatorMsg<void>>>) -> Task<void>;
extern template auto
  run_gather_signals(Box<Pull<OperatorMsg<table_slice>>>,
                     std::vector<Box<Pull<OperatorMsg<void>>>>,
                     Box<Push<OperatorMsg<table_slice>>>) -> Task<void>;
extern template auto
  run_gather_signals(Box<Pull<OperatorMsg<chunk_ptr>>>,
                     std::vector<Box<Pull<OperatorMsg<void>>>>,
                     Box<Push<OperatorMsg<chunk_ptr>>>) -> Task<void>;

/// Builds `lanes` internal channels, returning the per-lane pushes and pulls
/// as parallel vectors. `make_channel` produces one channel per lane, e.g.
/// `ExecCtx::make_channel<table_slice>`.
template <class Factory>
auto make_lane_channels(size_t lanes, Factory& make_channel, ChannelId id)
  -> std::pair<std::vector<Box<Push<OperatorMsg<table_slice>>>>,
               std::vector<Box<Pull<OperatorMsg<table_slice>>>>> {
  auto lane_pushes = std::vector<Box<Push<OperatorMsg<table_slice>>>>{};
  auto lane_pulls = std::vector<Box<Pull<OperatorMsg<table_slice>>>>{};
  lane_pushes.reserve(lanes);
  lane_pulls.reserve(lanes);
  for (auto lane = size_t{0}; lane < lanes; ++lane) {
    auto pair = make_channel(id);
    lane_pushes.push_back(std::move(pair.push));
    lane_pulls.push_back(std::move(pair.pull));
  }
  return {std::move(lane_pushes), std::move(lane_pulls)};
}

/// Creates a scatter exchange with `lanes` downstream lanes.
///
/// Returns the single upstream `Push` (a `ScatterPush`) and the `lanes` lane
/// `Pull`s. `make_channel` produces one internal SPSC channel per lane, e.g.
/// `ExecCtx::make_channel<table_slice>`.
template <class Factory>
auto make_scatter(size_t lanes, Factory make_channel, ChannelId id)
  -> std::pair<Box<Push<OperatorMsg<table_slice>>>,
               std::vector<Box<Pull<OperatorMsg<table_slice>>>>> {
  TENZIR_ASSERT(lanes > 0);
  auto [lane_pushes, lane_pulls] = make_lane_channels(lanes, make_channel, id);
  auto scatter
    = Box<Push<OperatorMsg<table_slice>>>{ScatterPush{std::move(lane_pushes)}};
  return {std::move(scatter), std::move(lane_pulls)};
}

/// Creates a broadcast exchange with `lanes` downstream lanes.
///
/// Returns the single upstream `Push` (a `BroadcastPush`) and the `lanes` lane
/// `Pull`s. `make_channel` produces one internal SPSC channel per lane, e.g.
/// `ExecCtx::make_channel<table_slice>`.
template <class Factory>
auto make_broadcast(size_t lanes, Factory make_channel, ChannelId id)
  -> std::pair<Box<Push<OperatorMsg<table_slice>>>,
               std::vector<Box<Pull<OperatorMsg<table_slice>>>>> {
  TENZIR_ASSERT(lanes > 0);
  auto [lane_pushes, lane_pulls] = make_lane_channels(lanes, make_channel, id);
  auto broadcast = Box<Push<OperatorMsg<table_slice>>>{
    BroadcastPush{std::move(lane_pushes)}};
  return {std::move(broadcast), std::move(lane_pulls)};
}

/// The parts of a gather exchange with `lanes` upstream lanes.
struct GatherParts {
  /// One `Push` per lane, held by each upstream lane operator.
  std::vector<Box<Push<OperatorMsg<table_slice>>>> lanes;
  /// The single downstream `Pull`, held by the downstream operator.
  Box<Pull<OperatorMsg<table_slice>>> pull;
  /// The merge loop; the caller must spawn this task.
  Task<void> merger;
};

/// Creates a shuffle exchange with `lanes` downstream lanes.
///
/// Returns the single upstream `Push` (a `ShufflePush`) and the `lanes` lane
/// `Pull`s. `make_channel` produces one internal SPSC channel per lane, e.g.
/// `ExecCtx::make_channel<table_slice>`. `keys` are the partition-key
/// expressions evaluated against each incoming slice; `dh` must outlive the
/// returned `Push`.
template <class Factory>
auto make_shuffle(size_t lanes, std::vector<ast::expression> keys,
                  diagnostic_handler& dh, Factory make_channel, ChannelId id)
  -> std::pair<Box<Push<OperatorMsg<table_slice>>>,
               std::vector<Box<Pull<OperatorMsg<table_slice>>>>> {
  TENZIR_ASSERT(lanes > 0);
  auto [lane_pushes, lane_pulls] = make_lane_channels(lanes, make_channel, id);
  auto shuffle = Box<Push<OperatorMsg<table_slice>>>{
    ShufflePush{std::move(lane_pushes), std::move(keys), dh}};
  return {std::move(shuffle), std::move(lane_pulls)};
}

/// Creates a gather exchange with `lanes` upstream lanes.
///
/// The caller is responsible for spawning `GatherParts::merger` on a suitable
/// scope. `make_channel` produces the profiled per-lane fan-in channels that
/// share `id`, e.g. `ExecCtx::make_routing_channel`.
///
/// The post-merge output channel that feeds the downstream operator is created
/// internally as a plain, unprofiled fused channel rather than through
/// `make_channel`. If it shared the lanes' `id`, every gathered row would be
/// metered twice (once on its lane, once on the merge output), double-counting
/// the exchange's throughput and adding a spurious per-lane limit to its
/// reported input capacity. Keeping it out of the profiled factory lets all
/// lanes collate into a single accurate metric under `id`.
template <class Factory>
auto make_gather(size_t lanes, Factory make_channel, ChannelId id)
  -> GatherParts {
  TENZIR_ASSERT(lanes > 0);
  auto [lane_pushes, lane_pulls] = make_lane_channels(lanes, make_channel, id);
  auto out = fused_channel<OperatorMsg<table_slice>>().into_push_pull();
  return GatherParts{
    .lanes = std::move(lane_pushes),
    .pull = std::move(out.pull),
    .merger = run_gather(std::move(lane_pulls), std::move(out.push)),
  };
}

} // namespace tenzir
