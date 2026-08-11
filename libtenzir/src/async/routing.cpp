//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/routing.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/option.hpp"

#include <algorithm>
#include <functional>
#include <numeric>
#include <span>

namespace tenzir::routing {

auto water_fill(uint64_t total, std::span<const size_t> sorted_indices,
                std::span<const uint64_t> rows_assigned)
  -> std::vector<uint64_t> {
  auto k = sorted_indices.size();
  auto alloc = std::vector<uint64_t>(k, 0);
  auto remaining = total;
  for (auto level = size_t{0}; level + 1 < k; ++level) {
    auto gap = rows_assigned[sorted_indices[level + 1]]
               - rows_assigned[sorted_indices[level]];
    auto needed = gap * (level + 1);
    if (needed <= remaining) {
      for (auto j = size_t{0}; j <= level; ++j) {
        alloc[j] += gap;
      }
      remaining -= needed;
    } else {
      auto per = remaining / (level + 1);
      auto extra = remaining % (level + 1);
      for (auto j = size_t{0}; j <= level; ++j) {
        alloc[j] += per + (j < extra ? 1 : 0);
      }
      remaining = 0;
      break;
    }
  }
  if (remaining > 0) {
    auto per = remaining / k;
    auto extra = remaining % k;
    for (auto j = size_t{0}; j < k; ++j) {
      alloc[j] += per + (j < extra ? 1 : 0);
    }
  }
  return alloc;
}

auto distribute_adaptive(uint64_t total_rows,
                         std::vector<uint64_t>& rows_assigned)
  -> std::vector<std::pair<size_t, uint64_t>> {
  auto n = rows_assigned.size();
  // Sort worker indices by rows_assigned ascending.
  auto sorted = std::vector<size_t>(n);
  std::iota(sorted.begin(), sorted.end(), size_t{0});
  std::sort(sorted.begin(), sorted.end(), [&](size_t a, size_t b) {
    return rows_assigned[a] < rows_assigned[b];
  });
  auto alloc = std::vector<uint64_t>{};
  for (auto k = size_t{1}; k <= n; ++k) {
    alloc = water_fill(total_rows, std::span{sorted.data(), k}, rows_assigned);
    if (k == n) {
      break;
    }
    // Check whether this distribution satisfies the fairness constraint.
    auto new_totals = rows_assigned;
    for (auto i = size_t{0}; i < k; ++i) {
      new_totals[sorted[i]] += alloc[i];
    }
    auto [min_it, max_it]
      = std::minmax_element(new_totals.begin(), new_totals.end());
    auto is_fair = static_cast<double>(*max_it)
                   <= static_cast<double>(*min_it) * fairness_factor;
    if (is_fair) {
      break;
    }
  }
  auto result = std::vector<std::pair<size_t, uint64_t>>{};
  for (auto i = size_t{0}; i < alloc.size(); ++i) {
    if (alloc[i] > 0) {
      rows_assigned[sorted[i]] += alloc[i];
      result.emplace_back(sorted[i], alloc[i]);
    }
  }
  return result;
}

namespace {

/// A contiguous run of rows `[begin, end)` that all hash to the same `bucket`.
struct HashRun {
  uint64_t bucket;
  int64_t begin;
  int64_t end;
};

/// The maximum number of runs per used bucket for which `hash_partition` keeps
/// the zero-copy sub-slice path instead of materializing one slice per bucket.
constexpr auto max_runs_per_bucket = size_t{2};

/// Hashes every row exactly once, yielding its bucket.
auto row_buckets(const multi_series& values, uint64_t jobs)
  -> std::vector<uint64_t> {
  TENZIR_ASSERT(jobs > 0);
  auto num_rows = values.length();
  auto result = std::vector<uint64_t>{};
  result.reserve(static_cast<size_t>(num_rows));
  for (auto row = int64_t{0}; row < num_rows; ++row) {
    result.push_back(std::hash<data_view3>{}(values.view3_at(row)) % jobs);
  }
  return result;
}

/// Derives the maximal contiguous same-bucket runs from a bucket vector, giving
/// up once their number exceeds `limit`.
///
/// Unclustered buckets yield one run per row, so the caller, which only wants
/// to know whether the input is clustered, must not pay for materializing them:
/// the limit makes it bail out after a short prefix instead of scanning and
/// allocating over every row.
auto runs_from_buckets(std::span<const uint64_t> buckets, size_t limit)
  -> Option<std::vector<HashRun>> {
  auto num_rows = detail::narrow<int64_t>(buckets.size());
  auto result = std::vector<HashRun>{};
  result.reserve(std::min(limit, static_cast<size_t>(num_rows)));
  auto begin = int64_t{0};
  while (begin < num_rows) {
    if (result.size() == limit) {
      return None{};
    }
    auto bucket = buckets[begin];
    auto end = begin + 1;
    while (end < num_rows and buckets[end] == bucket) {
      ++end;
    }
    result.push_back({bucket, begin, end});
    begin = end;
  }
  return result;
}

} // namespace

auto hash_partition(const table_slice& slice, const multi_series& keys,
                    uint64_t jobs) -> std::vector<RoutedSlice> {
  TENZIR_ASSERT(jobs > 0);
  TENZIR_ASSERT(keys.length() == detail::narrow<int64_t>(slice.rows()));
  auto num_rows = keys.length();
  if (num_rows == 0) {
    return {};
  }
  if (jobs == 1) {
    return {RoutedSlice{0, slice}};
  }
  auto buckets = row_buckets(keys, jobs);
  // One pass collects the row indices per bucket, so the total work is O(rows)
  // rather than the O(rows * jobs) of building one boolean mask per bucket.
  auto indices = std::vector<std::vector<int64_t>>(jobs);
  for (auto row = int64_t{0}; row < num_rows; ++row) {
    indices[buckets[row]].push_back(row);
  }
  auto used = std::ranges::count_if(indices, [](const auto& rows) {
    return not rows.empty();
  });
  // Everything lands in one bucket: forward the input untouched.
  if (used == 1) {
    return {RoutedSlice{buckets[0], slice}};
  }
  // The input is already clustered by bucket: sub-slices are cheaper than a
  // copy, and the number of parts stays bounded. Unclustered input bails out of
  // the run detection after a short prefix.
  if (auto runs = runs_from_buckets(buckets, max_runs_per_bucket
                                               * static_cast<size_t>(used))) {
    auto result = std::vector<RoutedSlice>{};
    result.reserve(runs->size());
    for (auto [bucket, begin, end] : *runs) {
      result.push_back({bucket, subslice(slice, begin, end)});
    }
    return result;
  }
  auto result = std::vector<RoutedSlice>{};
  result.reserve(static_cast<size_t>(used));
  for (auto bucket = uint64_t{0}; bucket < jobs; ++bucket) {
    if (indices[bucket].empty()) {
      continue;
    }
    result.push_back({bucket, take_rows(slice, indices[bucket])});
  }
  return result;
}

} // namespace tenzir::routing
