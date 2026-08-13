//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/routing.hpp"

#include "tenzir/async/channel.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/table_slice.hpp"

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/executors/GlobalExecutor.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/option.hpp"
#include "tenzir/test/test.hpp"

#include <functional>
#include <numeric>
#include <set>
#include <vector>

namespace tenzir {

namespace {

using namespace tenzir::routing;

/// Builds a slice with a `key` and an `id` field, one row per key. The `id`
/// field makes every row identifiable across a partitioning.
auto make_keyed_slice(std::span<const std::string> keys) -> table_slice {
  auto b = series_builder{};
  for (auto i = size_t{0}; i < keys.size(); ++i) {
    auto r = b.record();
    r.field("key").data(keys[i]);
    r.field("id").data(detail::narrow<int64_t>(i));
  }
  auto slices = b.finish_as_table_slice("test");
  TENZIR_ASSERT(slices.size() == 1);
  return std::move(slices[0]);
}

/// Returns the `key` column of a slice built by `make_keyed_slice`.
auto key_column(const table_slice& slice) -> multi_series {
  const auto& schema = as<record_type>(slice.schema());
  return multi_series{
    series{schema.field(0).type, to_record_batch(slice)->column(0)}};
}

/// Counts the maximal contiguous stretches of rows that hash to the same
/// bucket. This is what a run-based exchange would push as separate messages.
auto count_runs(const multi_series& values, uint64_t jobs) -> size_t {
  auto result = size_t{0};
  auto previous = Option<uint64_t>{};
  for (auto row = int64_t{0}; row < values.length(); ++row) {
    auto bucket = std::hash<data_view3>{}(values.view3_at(row)) % jobs;
    if (previous != bucket) {
      ++result;
      previous = bucket;
    }
  }
  return result;
}

/// Returns the `id` values of a slice built by `make_keyed_slice`, in order.
auto ids_of(const table_slice& slice) -> std::vector<int64_t> {
  auto result = std::vector<int64_t>{};
  const auto& array = as<arrow::Int64Array>(*to_record_batch(slice)->column(1));
  for (auto i = int64_t{0}; i < array.length(); ++i) {
    result.push_back(array.Value(i));
  }
  return result;
}

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

TEST("hash_partition routes equal keys to the same bucket") {
  // Keyed operators such as `summarize`, `group`, and `deduplicate` rely on
  // this invariant: two rows whose keys are the same value of the same type
  // must land on the same instance, or a single logical group would be split
  // across instances.
  //
  // Buckets are derived from `hash_append`, which tags values by type. Keys of
  // different types therefore route independently even when they compare
  // equivalent, e.g. `1` and `1.0`. That matches the keyed operators, which
  // hash their keys the same way and so also treat them as distinct groups.
  auto b = series_builder{};
  const auto keys = std::vector<data>{
    int64_t{1},
    int64_t{2},
    int64_t{1},
    std::string{"a"},
    std::string{"a"},
    std::string{"b"},
    caf::none,
    caf::none,
    record{{"x", int64_t{1}}},
    record{{"x", int64_t{1}}},
    record{{"x", int64_t{2}}},
    list{int64_t{1}, int64_t{2}},
    list{int64_t{1}, int64_t{2}},
  };
  for (const auto& key : keys) {
    b.data(key);
  }
  // The keys are heterogeneous, so this yields one part per type.
  auto values = multi_series{b.finish()};
  REQUIRE_EQUAL(values.length(), static_cast<int64_t>(keys.size()));
  const auto jobs = uint64_t{4};
  // The routing keys come from `values`; the slice only carries row ids.
  auto slice = make_keyed_slice(std::vector<std::string>(keys.size(), "row"));
  // Materialize the per-row bucket assignment from the returned parts.
  auto buckets = std::vector<uint64_t>(keys.size());
  for (const auto& part : hash_partition(slice, values, jobs)) {
    for (auto id : ids_of(part.slice)) {
      buckets[static_cast<size_t>(id)] = part.bucket;
    }
  }
  for (auto i = size_t{0}; i < keys.size(); ++i) {
    for (auto j = i + 1; j < keys.size(); ++j) {
      if (keys[i] == keys[j]) {
        CHECK_EQUAL(buckets[i], buckets[j]);
      }
    }
  }
}

TEST("hash_partition covers every row exactly once") {
  auto keys = std::vector<std::string>{};
  for (auto i = 0; i < 100; ++i) {
    keys.push_back(fmt::format("key-{}", i));
  }
  auto slice = make_keyed_slice(keys);
  auto parts = hash_partition(slice, key_column(slice), 4);
  auto seen = std::vector<int64_t>{};
  for (const auto& part : parts) {
    CHECK(part.bucket < uint64_t{4});
    auto part_ids = ids_of(part.slice);
    seen.insert(seen.end(), part_ids.begin(), part_ids.end());
  }
  std::sort(seen.begin(), seen.end());
  auto expected = std::vector<int64_t>(keys.size());
  std::iota(expected.begin(), expected.end(), int64_t{0});
  CHECK_EQUAL(seen, expected);
}

TEST("hash_partition emits at most one slice per bucket") {
  // The regression test for the shuffle exchange: routing contiguous runs
  // degenerates to one message per row for unclustered keys. `hash_partition`
  // must stay bounded by `jobs` regardless of how the keys are interleaved.
  auto keys = std::vector<std::string>{};
  for (auto i = 0; i < 1000; ++i) {
    keys.push_back(fmt::format("key-{}", i));
  }
  auto slice = make_keyed_slice(keys);
  const auto jobs = uint64_t{3};
  auto values = key_column(slice);
  // The keys are interleaved enough for a run-based split to fragment.
  CHECK(count_runs(values, jobs) > 100);
  auto parts = hash_partition(slice, values, jobs);
  CHECK(parts.size() <= jobs);
  auto buckets = std::set<uint64_t>{};
  for (const auto& part : parts) {
    CHECK(buckets.insert(part.bucket).second);
    CHECK(part.slice.rows() > 0);
  }
}

TEST("hash_partition preserves order within a bucket") {
  auto keys = std::vector<std::string>{"a", "b", "a", "b", "a", "b", "a"};
  auto slice = make_keyed_slice(keys);
  auto parts = hash_partition(slice, key_column(slice), 2);
  for (const auto& part : parts) {
    auto part_ids = ids_of(part.slice);
    CHECK(std::ranges::is_sorted(part_ids));
  }
}

TEST("hash_partition with one job returns the input unchanged") {
  auto keys = std::vector<std::string>{"a", "b", "c"};
  auto slice = make_keyed_slice(keys);
  auto parts = hash_partition(slice, key_column(slice), 1);
  REQUIRE_EQUAL(parts.size(), size_t{1});
  CHECK_EQUAL(parts[0].bucket, uint64_t{0});
  CHECK_EQUAL(parts[0].slice.rows(), slice.rows());
  CHECK_EQUAL(ids_of(parts[0].slice), (std::vector<int64_t>{0, 1, 2}));
}

TEST("hash_partition on empty input yields nothing") {
  auto parts = hash_partition(table_slice{}, multi_series{}, 4);
  CHECK(parts.empty());
}

TEST("hash_partition keeps clustered input in as few parts as buckets") {
  // Sort the keys by bucket so the input is already clustered; the result must
  // then have exactly one part per used bucket.
  auto keys = std::vector<std::string>{};
  for (auto i = 0; i < 60; ++i) {
    keys.push_back(fmt::format("key-{}", i));
  }
  const auto jobs = uint64_t{3};
  std::ranges::sort(keys, [&](const std::string& a, const std::string& b) {
    return std::hash<data_view3>{}(data_view3{std::string_view{a}}) % jobs
           < std::hash<data_view3>{}(data_view3{std::string_view{b}}) % jobs;
  });
  auto slice = make_keyed_slice(keys);
  auto values = key_column(slice);
  auto parts = hash_partition(slice, values, jobs);
  CHECK_EQUAL(parts.size(), count_runs(values, jobs));
  CHECK(parts.size() <= jobs);
}

TEST("hash_partition preserves slice metadata") {
  auto keys = std::vector<std::string>{};
  for (auto i = 0; i < 50; ++i) {
    keys.push_back(fmt::format("key-{}", i));
  }
  auto slice = make_keyed_slice(keys);
  const auto import_time = tenzir::time{std::chrono::seconds{1234567890}};
  slice.import_time(import_time);
  for (const auto& part : hash_partition(slice, key_column(slice), 4)) {
    CHECK_EQUAL(part.slice.schema(), slice.schema());
    CHECK_EQUAL(part.slice.import_time(), import_time);
  }
}

} // namespace tenzir
