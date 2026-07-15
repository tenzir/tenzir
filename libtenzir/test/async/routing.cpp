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

#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>
#include <folly/executors/GlobalExecutor.h>

#ifdef CHECK
#  undef CHECK
#endif
#include "tenzir/test/test.hpp"

#include <functional>
#include <numeric>
#include <set>
#include <vector>

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

/// A `Push` backed by a channel `Sender`. Dropping it closes the channel.
template <class T>
class SenderPush final : public Push<OperatorMsg<T>> {
public:
  explicit SenderPush(Sender<OperatorMsg<T>> sender)
    : sender_{std::move(sender)} {
  }

  auto operator()(OperatorMsg<T> x) -> Task<void> override {
    return sender_.send(std::move(x));
  }

private:
  Sender<OperatorMsg<T>> sender_;
};

/// A `Pull` backed by a channel `Receiver`.
template <class T>
class ReceiverPull final : public Pull<OperatorMsg<T>> {
public:
  explicit ReceiverPull(Receiver<OperatorMsg<T>> receiver)
    : receiver_{std::move(receiver)} {
  }

  auto operator()() -> Task<Option<OperatorMsg<T>>> override {
    return receiver_.recv();
  }

private:
  Receiver<OperatorMsg<T>> receiver_;
};

template <class T>
auto local_channel(ChannelId = ChannelId{}, size_t capacity = 128)
  -> PushPull<OperatorMsg<T>> {
  auto [sender, receiver] = channel<OperatorMsg<T>>(capacity);
  return PushPull<OperatorMsg<T>>{
    Box<Push<OperatorMsg<T>>>{SenderPush<T>{std::move(sender)}},
    Box<Pull<OperatorMsg<T>>>{ReceiverPull<T>{std::move(receiver)}},
  };
}

// A channel factory for scatter/gather exchanges.
auto slice_factory() {
  return [](ChannelId id) {
    return local_channel<table_slice>(std::move(id));
  };
}

// Builds a single `table_slice` with `rows` rows.
auto make_slice(int64_t rows) -> table_slice {
  auto b = series_builder{};
  for (auto i = int64_t{0}; i < rows; ++i) {
    b.record().field("x", i);
  }
  return b.finish_assert_one_slice();
}

// Runs a task body on the global CPU executor. The gather merge loop uses an
// `async_scope`, which spawns lane-pull tasks onto the current executor; those
// tasks must run concurrently with the merger blocking on `next()`, so we need
// a multi-threaded executor rather than the single-threaded `blockingWait` one.
template <class F>
auto run(F&& f) -> void {
  folly::coro::blockingWait(folly::coro::co_withExecutor(
    folly::getGlobalCPUExecutor(), std::forward<F>(f)()));
}

} // namespace

TEST("scatter distributes rows across lanes and broadcasts signals") {
  run([&]() -> Task<void> {
    auto [push, pulls] = make_scatter(2, slice_factory(), ChannelId{});
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        // Four single-row slices to two lanes: each lane should get two rows.
        for (auto i = 0; i < 4; ++i) {
          co_await (*push)(OperatorMsg<table_slice>{make_slice(1)});
        }
        co_await (*push)(OperatorMsg<table_slice>{Signal{EndOfData{}}});
        // Drop the scatter to close all lanes.
        push = {};
      }(),
      [&]() -> Task<void> {
        auto rows = int64_t{0};
        auto got_signal = false;
        while (auto msg = co_await (*pulls[0])()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            rows += slice->rows();
          } else {
            // The signal must be broadcast to this lane.
            check(is<Signal>(*msg));
            got_signal = true;
          }
        }
        check_eq(rows, int64_t{2});
        check(got_signal);
      }(),
      [&]() -> Task<void> {
        auto rows = int64_t{0};
        auto got_signal = false;
        while (auto msg = co_await (*pulls[1])()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            rows += slice->rows();
          } else {
            got_signal = true;
          }
        }
        check_eq(rows, int64_t{2});
        check(got_signal);
      }());
  });
}

TEST("scatter stops routing data to a closed lane") {
  run([&]() -> Task<void> {
    auto [push, pulls] = make_scatter(2, slice_factory(), ChannelId{});
    // Retire lane 1 up front.
    static_cast<ScatterPush&>(*push).close_lane(1);
    check_eq(static_cast<ScatterPush&>(*push).open_lanes(), size_t{1});
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        for (auto i = 0; i < 3; ++i) {
          co_await (*push)(OperatorMsg<table_slice>{make_slice(1)});
        }
        push = {};
      }(),
      [&]() -> Task<void> {
        auto rows = int64_t{0};
        while (auto msg = co_await (*pulls[0])()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            rows += slice->rows();
          }
        }
        check_eq(rows, int64_t{3});
      }(),
      [&]() -> Task<void> {
        auto rows = int64_t{0};
        while (auto msg = co_await (*pulls[1])()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            rows += slice->rows();
          }
        }
        check_eq(rows, int64_t{0});
      }());
  });
}

TEST("broadcast sends every slice to all lanes") {
  run([&]() -> Task<void> {
    auto [push, pulls] = make_broadcast(2, slice_factory(), ChannelId{});
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        // Four single-row slices; each lane should receive all four rows.
        for (auto i = 0; i < 4; ++i) {
          co_await (*push)(OperatorMsg<table_slice>{make_slice(1)});
        }
        co_await (*push)(OperatorMsg<table_slice>{Signal{EndOfData{}}});
        // Drop the broadcast to close all lanes.
        push = {};
      }(),
      [&]() -> Task<void> {
        auto rows = int64_t{0};
        auto got_signal = false;
        while (auto msg = co_await (*pulls[0])()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            rows += slice->rows();
          } else {
            check(is<Signal>(*msg));
            got_signal = true;
          }
        }
        check_eq(rows, int64_t{4});
        check(got_signal);
      }(),
      [&]() -> Task<void> {
        auto rows = int64_t{0};
        auto got_signal = false;
        while (auto msg = co_await (*pulls[1])()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            rows += slice->rows();
          } else {
            got_signal = true;
          }
        }
        check_eq(rows, int64_t{4});
        check(got_signal);
      }());
  });
}

TEST("broadcast stops routing data to a closed lane") {
  run([&]() -> Task<void> {
    auto [push, pulls] = make_broadcast(2, slice_factory(), ChannelId{});
    // Retire lane 1 up front.
    static_cast<BroadcastPush&>(*push).close_lane(1);
    check_eq(static_cast<BroadcastPush&>(*push).open_lanes(), size_t{1});
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        for (auto i = 0; i < 3; ++i) {
          co_await (*push)(OperatorMsg<table_slice>{make_slice(1)});
        }
        push = {};
      }(),
      [&]() -> Task<void> {
        auto rows = int64_t{0};
        while (auto msg = co_await (*pulls[0])()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            rows += slice->rows();
          }
        }
        // The open lane still receives every slice.
        check_eq(rows, int64_t{3});
      }(),
      [&]() -> Task<void> {
        auto rows = int64_t{0};
        while (auto msg = co_await (*pulls[1])()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            rows += slice->rows();
          }
        }
        check_eq(rows, int64_t{0});
      }());
  });
}

TEST("gather interleaves data from all lanes") {
  run([&]() -> Task<void> {
    auto parts = make_gather(3, slice_factory(), ChannelId{});
    co_await folly::coro::collectAll(
      std::move(parts.merger),
      [lane = std::move(parts.lanes[0])]() mutable -> Task<void> {
        for (auto i = 0; i < 3; ++i) {
          co_await (*lane)(OperatorMsg<table_slice>{make_slice(1)});
        }
        // Drop the lane to close its channel; otherwise the captured push
        // outlives the coroutine until `collectAll` completes, which never
        // happens because the merger waits for every lane to close.
        lane = {};
      }(),
      [lane = std::move(parts.lanes[1])]() mutable -> Task<void> {
        for (auto i = 0; i < 3; ++i) {
          co_await (*lane)(OperatorMsg<table_slice>{make_slice(1)});
        }
        lane = {};
      }(),
      [lane = std::move(parts.lanes[2])]() mutable -> Task<void> {
        for (auto i = 0; i < 3; ++i) {
          co_await (*lane)(OperatorMsg<table_slice>{make_slice(1)});
        }
        lane = {};
      }(),
      [&]() -> Task<void> {
        auto total_rows = int64_t{0};
        while (auto msg = co_await (*parts.pull)()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            total_rows += slice->rows();
          }
        }
        check_eq(total_rows, int64_t{9});
      }());
  });
}

TEST("gather emits end-of-data exactly once after all lanes deliver it") {
  run([&]() -> Task<void> {
    auto parts = make_gather(2, slice_factory(), ChannelId{});
    co_await folly::coro::collectAll(
      std::move(parts.merger),
      [lane = std::move(parts.lanes[0])]() mutable -> Task<void> {
        co_await (*lane)(OperatorMsg<table_slice>{make_slice(1)});
        co_await (*lane)(OperatorMsg<table_slice>{Signal{EndOfData{}}});
        lane = {};
      }(),
      [lane = std::move(parts.lanes[1])]() mutable -> Task<void> {
        co_await (*lane)(OperatorMsg<table_slice>{make_slice(1)});
        co_await (*lane)(OperatorMsg<table_slice>{Signal{EndOfData{}}});
        lane = {};
      }(),
      [&]() -> Task<void> {
        auto data_rows = int64_t{0};
        auto eod = 0;
        while (auto msg = co_await (*parts.pull)()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            data_rows += slice->rows();
          } else {
            check(is<Signal>(*msg));
            check(is<EndOfData>(as<Signal>(*msg)));
            ++eod;
          }
        }
        check_eq(data_rows, int64_t{2});
        check_eq(eod, 1);
      }());
  });
}

TEST("scatter keeps a fair slice on one lane after closing a lane") {
  // Regression test: a retired lane must not skew the adaptive fairness check.
  // With four lanes and lane 3 closed, sending 300 rows levels the three open
  // lanes to [100, 100, 100]. A subsequent 10-row slice stays within the
  // fairness factor on a single lane instead of fragmenting across all three.
  run([&]() -> Task<void> {
    auto [push, pulls] = make_scatter(4, slice_factory(), ChannelId{});
    static_cast<ScatterPush&>(*push).close_lane(3);
    co_await folly::coro::collectAll(
      [&]() -> Task<void> {
        co_await (*push)(OperatorMsg<table_slice>{make_slice(300)});
        co_await (*push)(OperatorMsg<table_slice>{make_slice(10)});
        push = {};
      }(),
      [&]() -> Task<void> {
        auto rows_per_lane = std::multiset<int64_t>{};
        for (auto& pull : pulls) {
          auto rows = int64_t{0};
          while (auto msg = co_await (*pull)()) {
            if (auto* slice = try_as<table_slice>(*msg)) {
              rows += slice->rows();
            }
          }
          rows_per_lane.insert(rows);
        }
        // Lane 3 is retired (0 rows); the 10-row slice lands entirely on one
        // of the three open lanes, taking it to 110.
        auto expected = std::multiset<int64_t>{0, 100, 100, 110};
        check(rows_per_lane == expected);
      }());
  });
}

TEST("gather drains without end-of-data") {
  run([&]() -> Task<void> {
    auto parts = make_gather(2, slice_factory(), ChannelId{});
    co_await folly::coro::collectAll(
      std::move(parts.merger),
      [lane = std::move(parts.lanes[0])]() mutable -> Task<void> {
        co_await (*lane)(OperatorMsg<table_slice>{make_slice(5)});
        lane = {};
      }(),
      [lane = std::move(parts.lanes[1])]() mutable -> Task<void> {
        lane = {};
        co_return;
      }(),
      [&]() -> Task<void> {
        auto total_rows = int64_t{0};
        while (auto msg = co_await (*parts.pull)()) {
          if (auto* slice = try_as<table_slice>(*msg)) {
            total_rows += slice->rows();
          }
        }
        check_eq(total_rows, int64_t{5});
      }());
  });
}

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
