//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/async/routing.hpp"

#include "tenzir/async/select_set.hpp"
#include "tenzir/co_match.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/panic.hpp"
#include "tenzir/tql2/eval.hpp"

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

namespace tenzir {

ExchangePush::ExchangePush(
  std::vector<Box<Push<OperatorMsg<table_slice>>>> lanes)
  : lanes_{std::move(lanes)} {
  TENZIR_ASSERT(not lanes_.empty());
}

auto ExchangePush::operator()(OperatorMsg<table_slice> msg) -> Task<void> {
  // Note the `co_await`: `operator()` must itself be a coroutine so the
  // handler lambda temporaries created by `co_match` stay alive across the
  // suspension. A plain `return co_match(...)` would destroy them at the end
  // of the full expression, leaving the returned (lazily-started) handler
  // coroutine with a dangling reference to its captured `this`.
  co_await co_match(
    std::move(msg),
    [this](Signal signal) -> Task<void> {
      // Broadcast signals to every lane, sequentially.
      for (auto& lane : lanes_) {
        co_await (*lane)(OperatorMsg<table_slice>{signal});
      }
    },
    [this](table_slice data) -> Task<void> {
      co_await route_data(std::move(data));
    });
}

ScatterPush::ScatterPush(std::vector<Box<Push<OperatorMsg<table_slice>>>> lanes)
  : ExchangePush{std::move(lanes)}, rows_assigned_(lanes_.size(), 0) {
}

auto ScatterPush::route_data(table_slice data) -> Task<void> {
  // Split the slice across lanes by row, keeping load balanced.
  auto total = static_cast<uint64_t>(data.rows());
  if (total == 0) {
    co_return;
  }
  // `distribute_adaptive` updates `rows_assigned_` in place for the lanes it
  // fills and returns (lane, count) pairs for the non-empty assignments.
  auto assignments = routing::distribute_adaptive(total, rows_assigned_);
  auto offset = size_t{0};
  for (auto [lane, count] : assignments) {
    auto slice = subslice(data, offset, offset + count);
    offset += count;
    co_await (*lanes_[lane])(OperatorMsg<table_slice>{std::move(slice)});
  }
}

BroadcastPush::BroadcastPush(
  std::vector<Box<Push<OperatorMsg<table_slice>>>> lanes)
  : ExchangePush{std::move(lanes)} {
}

auto BroadcastPush::route_data(table_slice data) -> Task<void> {
  // Broadcast a copy of the whole slice to every lane. Unlike scatter, no
  // partitioning happens: each lane receives all rows. Blocking on a slow
  // lane applies backpressure to the upstream.
  for (auto& lane : lanes_) {
    co_await (*lane)(OperatorMsg<table_slice>{data});
  }
}

namespace {

/// Wraps `keys` into a single expression suitable for one-shot evaluation.
///
/// A single key is used as-is. Multiple keys are combined into an
/// `ast::record` so that hashing a row over the composite is a single
/// `hash<data_view3>` call in `hash_runs`.
auto combine_keys(std::vector<ast::expression> keys) -> ast::expression {
  TENZIR_ASSERT(not keys.empty());
  if (keys.size() == 1) {
    return std::move(keys.front());
  }
  auto items = std::vector<ast::record::item>{};
  items.reserve(keys.size());
  for (auto i = size_t{0}; i < keys.size(); ++i) {
    auto name = ast::identifier{fmt::format("{}", i), location::unknown};
    items.emplace_back(ast::record::field{std::move(name), std::move(keys[i])});
  }
  return ast::expression{
    ast::record{location::unknown, std::move(items), location::unknown}};
}

} // namespace

ShufflePush::ShufflePush(std::vector<Box<Push<OperatorMsg<table_slice>>>> lanes,
                         std::vector<ast::expression> keys,
                         diagnostic_handler& dh)
  : ExchangePush{std::move(lanes)},
    key_{combine_keys(std::move(keys))},
    dh_{&dh} {
}

auto ShufflePush::route_data(table_slice data) -> Task<void> {
  if (data.rows() == 0) {
    co_return;
  }
  auto values = eval(key_, data, *dh_);
  TENZIR_ASSERT(values.length() == detail::narrow<int64_t>(data.rows()));
  auto jobs = static_cast<uint64_t>(lanes_.size());
  for (auto [bucket, begin, end] : routing::hash_runs(values, jobs)) {
    auto slice = subslice(data, begin, end);
    co_await (*lanes_[bucket])(OperatorMsg<table_slice>{std::move(slice)});
  }
}

auto run_gather(std::vector<Box<Pull<OperatorMsg<table_slice>>>> lanes,
                Box<Push<OperatorMsg<table_slice>>> out) -> Task<void> {
  struct LaneMsg {
    size_t lane;
    Option<OperatorMsg<table_slice>> msg;
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
        [&](table_slice data) -> Task<bool> {
          co_await (*out)(OperatorMsg<table_slice>{std::move(data)});
          arm(lane);
          co_return false;
        },
        [&](Signal signal) -> Task<bool> {
          co_return co_await co_match(
            std::move(signal),
            [&](EndOfData) -> Task<bool> {
              // Emit a single aligned end-of-data once all lanes delivered it.
              if (++eod_count == n) {
                co_await (*out)(OperatorMsg<table_slice>{Signal{EndOfData{}}});
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

} // namespace tenzir
