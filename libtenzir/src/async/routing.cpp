//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/routing.hpp"

#include "tenzir/detail/assert.hpp"

#include <algorithm>
#include <functional>
#include <numeric>

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

auto hash_runs(const multi_series& values, uint64_t jobs)
  -> std::vector<hash_run> {
  TENZIR_ASSERT(jobs > 0);
  auto result = std::vector<hash_run>{};
  auto num_rows = values.length();
  auto begin = int64_t{0};
  while (begin < num_rows) {
    auto bucket = std::hash<data_view3>{}(values.view3_at(begin)) % jobs;
    auto end = begin + 1;
    while (end < num_rows
           and std::hash<data_view3>{}(values.view3_at(end)) % jobs == bucket) {
      ++end;
    }
    result.push_back({bucket, begin, end});
    begin = end;
  }
  return result;
}

} // namespace tenzir::routing
