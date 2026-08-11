//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/multi_series.hpp"
#include "tenzir/table_slice.hpp"

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

/// A part of an input slice destined for one exchange lane.
struct RoutedSlice {
  uint64_t bucket;
  table_slice slice;
};

/// Partitions `slice` into at most `jobs` parts, one per bucket, where
/// `bucket = hash(keys[row]) % jobs`. Rows keep their relative order within a
/// bucket; order across buckets is not preserved. Empty buckets are omitted.
///
/// This is the routing primitive behind hash-partitioned exchanges: each part
/// is pushed as a single slice to the lane owning its bucket. The number of
/// parts does not depend on how the keys are interleaved. Inputs that are
/// already clustered by bucket are returned as zero-copy sub-slices; otherwise
/// each part is materialized once.
///
/// Requires `jobs > 0` and `keys.length() == slice.rows()`.
auto hash_partition(const table_slice& slice, const multi_series& keys,
                    uint64_t jobs) -> std::vector<RoutedSlice>;

} // namespace tenzir::routing
