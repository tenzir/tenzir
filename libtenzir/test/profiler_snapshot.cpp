//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/profiler_snapshot.hpp"

#include "tenzir/defaults.hpp"
#include "tenzir/test/test.hpp"

#include <algorithm>
#include <chrono>
#include <string_view>

namespace tenzir {
namespace {

/// The nominal sampling interval, and the CPU time that amounts to a full core
/// over that interval.
constexpr auto interval = duration{defaults::metrics_interval};
constexpr auto interval_ns
  = std::chrono::duration_cast<std::chrono::nanoseconds>(interval).count();

/// Builds a channel profile for the edge `sender -> receiver`, attributing
/// `events` events and `bytes` bytes that were both pushed and pulled.
auto make_channel(std::string_view sender, std::string_view receiver,
                  size_t events, size_t bytes) -> ChannelProfile {
  auto stats = Arc<ChannelStats>{std::in_place};
  stats->in.events.store(events);
  stats->out.events.store(events);
  stats->in.bytes.store(bytes);
  stats->out.bytes.store(bytes);
  stats->in.batches.store(1);
  stats->out.batches.store(1);
  return ChannelProfile{
    .id = OpId{std::string{sender}}.to(OpId{std::string{receiver}}),
    .stats = std::move(stats),
    .type = element_type_tag{tag_v<table_slice>},
  };
}

/// Builds an executor profile with the given cumulative CPU time.
auto make_executor(std::string_view id, std::string_view name, int64_t cpu_ns,
                   size_t task_count = 1) -> ExecutorProfile {
  auto stats = Arc<ExecutorStats>{std::in_place};
  stats->cpu_ns.store(cpu_ns);
  stats->wall_ns.store(cpu_ns);
  stats->task_count.store(task_count);
  return ExecutorProfile{
    .id = OpId{std::string{id}},
    .stats = std::move(stats),
    .name = std::string{name},
  };
}

/// Looks up the entry for `id` in a snapshot, or fails the test.
auto entry_for(ProfilerSnapshot const& snapshot, std::string_view id)
  -> OperatorProfileEntry const& {
  auto it = std::ranges::find_if(snapshot.operators, [&](auto const& op) {
    return op.operator_id == id;
  });
  REQUIRE(it != snapshot.operators.end());
  return *it;
}

auto has_entry(ProfilerSnapshot const& snapshot, std::string_view id) -> bool {
  return std::ranges::any_of(snapshot.operators, [&](auto const& op) {
    return op.operator_id == id;
  });
}

auto no_negative_cpu(ProfilerSnapshot const& snapshot) -> bool {
  return std::ranges::all_of(snapshot.operators, [](auto const& op) {
    return op.cpu >= 0.0;
  });
}

TEST("cpu is the fraction of the elapsed time spent on cpu") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto channels = std::vector{make_channel("_", "0", 10, 100)};
  // Half of the interval on CPU.
  auto executors = std::vector{make_executor("0", "pass", interval_ns / 2)};
  auto snapshot
    = build_profiler_snapshot(channels, executors, time{}, interval, prev);
  REQUIRE_EQUAL(snapshot.operators.size(), 1u);
  auto const& entry = entry_for(snapshot, "0");
  CHECK_EQUAL(entry.name, "pass");
  CHECK(entry.cpu > 49.0);
  CHECK(entry.cpu < 51.0);
}

// A tick that arrives late covers more than one nominal interval. Dividing by
// the nominal interval would inflate `cpu` in proportion to the delay, and
// because this task shares its executor with the pipeline it observes, ticks
// are late exactly when the pipeline is busy.
TEST("a late tick does not inflate cpu") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto channels = std::vector{make_channel("_", "0", 10, 100)};
  auto executors = std::vector{make_executor("0", "pass", interval_ns)};
  // A full interval of CPU time, but two intervals actually passed: the
  // operator used half a core, not a full one.
  auto snapshot
    = build_profiler_snapshot(channels, executors, time{}, 2 * interval, prev);
  auto const& entry = entry_for(snapshot, "0");
  CHECK(entry.cpu > 49.0);
  CHECK(entry.cpu < 51.0);
}

TEST("a short tick reports the higher rate it observed") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto channels = std::vector{make_channel("_", "0", 10, 100)};
  auto executors = std::vector{make_executor("0", "pass", interval_ns / 2)};
  // Half an interval of CPU time over a quarter of an interval: two cores.
  auto snapshot
    = build_profiler_snapshot(channels, executors, time{}, interval / 4, prev);
  auto const& entry = entry_for(snapshot, "0");
  CHECK(entry.cpu > 199.0);
  CHECK(entry.cpu < 201.0);
}

// `duration` is the contract that lets a consumer turn the delta counters into
// rates. If it ever drifted from the value that divides `cpu`, the row would
// contradict itself: a consumer recomputing CPU usage from the counters and
// the window would get a different answer than the engine reported.
TEST("the reported duration is the divisor used for cpu") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto channels = std::vector{make_channel("_", "0", 10, 100)};
  auto executors = std::vector{make_executor("0", "pass", interval_ns)};
  // Deliberately neither the nominal interval nor a multiple of it.
  auto window = duration{std::chrono::milliseconds{1337}};
  auto snapshot
    = build_profiler_snapshot(channels, executors, time{}, window, prev);
  REQUIRE_EQUAL(snapshot.duration, window);
  auto const& entry = entry_for(snapshot, "0");
  auto reported_ns
    = std::chrono::duration_cast<std::chrono::nanoseconds>(snapshot.duration)
        .count();
  auto recomputed = static_cast<double>(interval_ns)
                    / static_cast<double>(reported_ns) * 100.0;
  CHECK_EQUAL(entry.cpu, recomputed);
}

TEST("duration is reported even without operators") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto window = duration{std::chrono::milliseconds{500}};
  auto snapshot = build_profiler_snapshot({}, {}, time{}, window, prev);
  CHECK_EQUAL(snapshot.duration, window);
}

TEST("cpu is zero when no time elapsed") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto channels = std::vector{make_channel("_", "0", 10, 100)};
  auto executors = std::vector{make_executor("0", "pass", interval_ns)};
  auto snapshot = build_profiler_snapshot(channels, executors, time{},
                                          duration::zero(), prev);
  auto const& entry = entry_for(snapshot, "0");
  CHECK_EQUAL(entry.cpu, 0.0);
  // The other counters are unaffected by the divisor.
  CHECK_EQUAL(entry.events_in, 10u);
}

TEST("counters are reported as deltas against the previous tick") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  {
    auto channels = std::vector{make_channel("_", "0", 10, 100)};
    auto executors = std::vector{make_executor("0", "pass", interval_ns)};
    auto snapshot
      = build_profiler_snapshot(channels, executors, time{}, interval, prev);
    auto const& entry = entry_for(snapshot, "0");
    CHECK_EQUAL(entry.events_in, 10u);
    CHECK(entry.cpu > 99.0);
  }
  {
    // Cumulative counters grew by 5 events and half an interval of CPU.
    auto channels = std::vector{make_channel("_", "0", 15, 150)};
    auto executors
      = std::vector{make_executor("0", "pass", interval_ns + interval_ns / 2)};
    auto snapshot
      = build_profiler_snapshot(channels, executors, time{}, interval, prev);
    auto const& entry = entry_for(snapshot, "0");
    CHECK_EQUAL(entry.events_in, 5u);
    CHECK(entry.cpu > 49.0);
    CHECK(entry.cpu < 51.0);
  }
}

// This is the regression test for the negative `cpu` readings observed during
// graceful shutdown. A terminated operator has its `ExecutorStats` reported one
// final time and then reaped, while the channels adjacent to it are co-owned by
// its neighbors and keep being reported until they drain. The snapshot for such
// an operator must be dropped rather than diffed against zero.
TEST("operators without an executor profile are not reported") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto channels = std::vector{
    make_channel("_", "0", 10, 100),
    make_channel("0", "1", 10, 100),
  };
  {
    auto executors = std::vector{
      make_executor("0", "pass", interval_ns / 2),
      make_executor("1", "discard", interval_ns / 4),
    };
    auto snapshot
      = build_profiler_snapshot(channels, executors, time{}, interval, prev);
    CHECK_EQUAL(snapshot.operators.size(), 2u);
    CHECK(entry_for(snapshot, "0").cpu > 0.0);
    CHECK(entry_for(snapshot, "1").cpu > 0.0);
  }
  {
    // Operator 0 terminated: its executor is gone, but the channels on both
    // sides are still reported.
    auto executors
      = std::vector{make_executor("1", "discard", interval_ns / 4)};
    auto snapshot
      = build_profiler_snapshot(channels, executors, time{}, interval, prev);
    CHECK(no_negative_cpu(snapshot));
    CHECK(not has_entry(snapshot, "0"));
    REQUIRE(has_entry(snapshot, "1"));
    CHECK_EQUAL(entry_for(snapshot, "1").cpu, 0.0);
    // The stale snapshot must be pruned so that a later tick cannot diff
    // against it either.
    CHECK(not prev.contains(OpId{"0"}));
  }
  {
    auto executors
      = std::vector{make_executor("1", "discard", interval_ns / 4)};
    auto snapshot
      = build_profiler_snapshot(channels, executors, time{}, interval, prev);
    CHECK(no_negative_cpu(snapshot));
    CHECK(not has_entry(snapshot, "0"));
  }
}

// An executor whose profile was reaped and then recreated for the same operator
// starts its counters over at zero, so the cumulative CPU time goes backwards.
// That means a restart, not negative usage: the current value is what accrued
// since. Every other counter has always been read this way via `delta()`; `cpu`
// used to be the one exception and reported a negative percentage instead.
TEST("an executor restarting from zero reports the cpu used since") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto channels = std::vector{make_channel("_", "0", 10, 100)};
  auto executors = std::vector{make_executor("0", "pass", interval_ns)};
  build_profiler_snapshot(channels, executors, time{}, interval, prev);
  auto restarted = std::vector{make_executor("0", "pass", interval_ns / 4)};
  auto snapshot
    = build_profiler_snapshot(channels, restarted, time{}, interval, prev);
  REQUIRE(has_entry(snapshot, "0"));
  auto const& entry = entry_for(snapshot, "0");
  CHECK(entry.cpu > 24.0);
  CHECK(entry.cpu < 26.0);
}

TEST("an executor restarting without cpu time reports zero") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto channels = std::vector{make_channel("_", "0", 10, 100)};
  auto executors = std::vector{make_executor("0", "pass", interval_ns)};
  build_profiler_snapshot(channels, executors, time{}, interval, prev);
  auto restarted = std::vector{make_executor("0", "pass", 0)};
  auto snapshot
    = build_profiler_snapshot(channels, restarted, time{}, interval, prev);
  REQUIRE(has_entry(snapshot, "0"));
  CHECK_EQUAL(entry_for(snapshot, "0").cpu, 0.0);
}

TEST("boundary channels are not attributed to a placeholder operator") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto channels = std::vector{
    make_channel("_", "0", 10, 100),
    make_channel("0", "_", 10, 100),
  };
  auto executors = std::vector{make_executor("0", "pass", interval_ns / 2)};
  auto snapshot
    = build_profiler_snapshot(channels, executors, time{}, interval, prev);
  CHECK_EQUAL(snapshot.operators.size(), 1u);
  CHECK(not has_entry(snapshot, "_"));
  auto const& entry = entry_for(snapshot, "0");
  CHECK_EQUAL(entry.events_in, 10u);
  CHECK_EQUAL(entry.events_out, 10u);
}

TEST("cross-boundary channels are attributed to the child operator") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  // `0-0/0` is an operator inside the sub-pipeline of operator `0`.
  auto channels = std::vector{make_channel("0", "0-0/0", 10, 100)};
  auto executors = std::vector{
    make_executor("0", "every", interval_ns / 2),
    make_executor("0-0/0", "pass", interval_ns / 2),
  };
  auto snapshot
    = build_profiler_snapshot(channels, executors, time{}, interval, prev);
  // The parent must not be credited with output for the channel that feeds
  // its own sub-pipeline.
  CHECK_EQUAL(entry_for(snapshot, "0").events_out, 0u);
  CHECK_EQUAL(entry_for(snapshot, "0-0/0").events_in, 10u);
}

TEST("timestamp is propagated") {
  auto prev = std::unordered_map<OpId, OpSnapshot>{};
  auto now = time{std::chrono::seconds{1234}};
  auto snapshot = build_profiler_snapshot({}, {}, now, interval, prev);
  CHECK_EQUAL(snapshot.timestamp, now);
  CHECK(snapshot.operators.empty());
}

} // namespace
} // namespace tenzir
