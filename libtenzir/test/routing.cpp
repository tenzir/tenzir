//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/routing.hpp"

#include "tenzir/series_builder.hpp"
#include "tenzir/test/test.hpp"

#include <functional>
#include <numeric>

namespace tenzir {

namespace {

using namespace tenzir::routing;

auto make_sorted(std::span<const uint64_t> rows_assigned)
  -> std::vector<size_t> {
  auto sorted = std::vector<size_t>(rows_assigned.size());
  std::iota(sorted.begin(), sorted.end(), size_t{0});
  std::sort(sorted.begin(), sorted.end(), [&](size_t a, size_t b) {
    return rows_assigned[a] < rows_assigned[b];
  });
  return sorted;
}

} // namespace

TEST("water_fill levels up the least-loaded workers first") {
  auto rows_assigned = std::vector<uint64_t>{100, 300, 500};
  auto sorted = make_sorted(rows_assigned);
  auto alloc = water_fill(1000, sorted, rows_assigned);
  // See the worked example in the documentation of `water_fill`.
  CHECK_EQUAL(alloc, (std::vector<uint64_t>{534, 333, 133}));
}

TEST("water_fill with equal load splits evenly") {
  auto rows_assigned = std::vector<uint64_t>{0, 0, 0, 0};
  auto sorted = make_sorted(rows_assigned);
  auto alloc = water_fill(1000, sorted, rows_assigned);
  CHECK_EQUAL(alloc, (std::vector<uint64_t>{250, 250, 250, 250}));
}

TEST("water_fill conserves the total across all allocations") {
  auto rows_assigned = std::vector<uint64_t>{7, 3, 11, 0, 5};
  auto sorted = make_sorted(rows_assigned);
  for (auto total : {uint64_t{0}, uint64_t{1}, uint64_t{13}, uint64_t{999}}) {
    auto alloc = water_fill(total, sorted, rows_assigned);
    auto sum = std::accumulate(alloc.begin(), alloc.end(), uint64_t{0});
    CHECK_EQUAL(sum, total);
  }
}

TEST("water_fill with a single worker gets everything") {
  auto rows_assigned = std::vector<uint64_t>{42};
  auto sorted = make_sorted(rows_assigned);
  auto alloc = water_fill(1000, sorted, rows_assigned);
  CHECK_EQUAL(alloc, (std::vector<uint64_t>{1000}));
}

TEST("distribute_adaptive from empty spreads across all workers") {
  auto rows_assigned = std::vector<uint64_t>{0, 0, 0, 0};
  auto result = distribute_adaptive(1000, rows_assigned);
  CHECK_EQUAL(rows_assigned, (std::vector<uint64_t>{250, 250, 250, 250}));
  // All four workers received rows.
  CHECK_EQUAL(result.size(), size_t{4});
  auto sum = uint64_t{0};
  for (auto [worker, count] : result) {
    sum += count;
  }
  CHECK_EQUAL(sum, uint64_t{1000});
}

TEST("distribute_adaptive prefers few workers while staying fair") {
  // 4 workers at [500, 300, 200, 100], distributing 400 rows. k=1 would be
  // unfair (max/min = 2.5), so the second-smallest worker joins.
  auto rows_assigned = std::vector<uint64_t>{500, 300, 200, 100};
  auto result = distribute_adaptive(400, rows_assigned);
  // Water-fill first closes the 100-row gap between workers 3 and 2, then
  // splits the remaining 300 evenly: worker 3 gets 250, worker 2 gets 150.
  CHECK_EQUAL(rows_assigned, (std::vector<uint64_t>{500, 300, 350, 350}));
  // Only workers 2 and 3 (the two least-loaded) received rows.
  CHECK_EQUAL(result.size(), size_t{2});
  auto sum = uint64_t{0};
  for (auto [worker, count] : result) {
    CHECK(worker == 2 or worker == 3);
    sum += count;
  }
  CHECK_EQUAL(sum, uint64_t{400});
}

TEST("distribute_adaptive sends everything to a single starved worker") {
  auto rows_assigned = std::vector<uint64_t>{100, 100, 100, 0};
  auto result = distribute_adaptive(50, rows_assigned);
  // Worker 3 can absorb all 50 rows while staying within the fairness factor.
  REQUIRE_EQUAL(result.size(), size_t{1});
  CHECK_EQUAL(result[0].first, size_t{3});
  CHECK_EQUAL(result[0].second, uint64_t{50});
  CHECK_EQUAL(rows_assigned, (std::vector<uint64_t>{100, 100, 100, 50}));
}

TEST("distribute_adaptive omits zero-row assignments") {
  auto rows_assigned = std::vector<uint64_t>{0, 0, 0};
  auto result = distribute_adaptive(0, rows_assigned);
  CHECK(result.empty());
  CHECK_EQUAL(rows_assigned, (std::vector<uint64_t>{0, 0, 0}));
}

TEST("hash_runs partitions all rows in order") {
  auto b = series_builder{};
  for (auto v : {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}) {
    b.data(int64_t{v});
  }
  auto values = multi_series{b.finish_assert_one_array()};
  const auto jobs = uint64_t{3};
  auto runs = hash_runs(values, jobs);
  REQUIRE(not runs.empty());
  // Runs cover [0, length) contiguously and in order.
  CHECK_EQUAL(runs.front().begin, int64_t{0});
  CHECK_EQUAL(runs.back().end, values.length());
  for (auto i = size_t{0}; i < runs.size(); ++i) {
    CHECK(runs[i].begin < runs[i].end);
    CHECK(runs[i].bucket < jobs);
    if (i + 1 < runs.size()) {
      // Contiguous and maximal: adjacent runs abut and differ in bucket.
      CHECK_EQUAL(runs[i].end, runs[i + 1].begin);
      CHECK(runs[i].bucket != runs[i + 1].bucket);
    }
  }
  // Every row's bucket matches hash(value) % jobs.
  for (const auto& run : runs) {
    for (auto row = run.begin; row < run.end; ++row) {
      auto expected = std::hash<data_view3>{}(values.view3_at(row)) % jobs;
      CHECK_EQUAL(run.bucket, expected);
    }
  }
}

TEST("hash_runs with one job yields a single run") {
  auto b = series_builder{};
  for (auto v : {1, 2, 3, 4, 5}) {
    b.data(int64_t{v});
  }
  auto values = multi_series{b.finish_assert_one_array()};
  auto runs = hash_runs(values, 1);
  REQUIRE_EQUAL(runs.size(), size_t{1});
  CHECK_EQUAL(runs[0].bucket, uint64_t{0});
  CHECK_EQUAL(runs[0].begin, int64_t{0});
  CHECK_EQUAL(runs[0].end, int64_t{5});
}

TEST("hash_runs on empty input yields no runs") {
  auto values = multi_series{};
  auto runs = hash_runs(values, 4);
  CHECK(runs.empty());
}

} // namespace tenzir
