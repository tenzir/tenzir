//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/profiler_snapshot.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/option.hpp"

#include <algorithm>

namespace tenzir {

auto build_profiler_snapshot(std::span<ChannelProfile const> channel_profiles,
                             std::span<ExecutorProfile const> executor_profiles,
                             time timestamp, duration elapsed,
                             std::unordered_map<OpId, OpSnapshot>& prev)
  -> ProfilerSnapshot {
  // Per-operator stats collected from channels and executors. Each group of
  // fields is written exactly once via `set()`.
  struct OpStats {
    std::string name;
    Option<size_t> input_bytes;
    Option<size_t> input_capacity;
    Option<size_t> bytes_in;
    Option<size_t> bytes_out;
    Option<size_t> batches_in;
    Option<size_t> batches_out;
    Option<size_t> events_in;
    Option<size_t> events_out;
    Option<size_t> signals_in;
    Option<size_t> signals_out;
    Option<int64_t> cpu_ns;
    Option<int64_t> wall_ns;
    Option<size_t> task_count;
  };
  auto set = []<class T>(Option<T>& field, T value) {
    if (field.is_none()) {
      field = value;
    } else {
      *field += value;
    }
  };
  auto ops = std::unordered_map<OpId, OpStats>{};
  auto is_child_of = [](OpId const& child, OpId const& parent) -> bool {
    return child.value.size() > parent.value.size()
           and child.value.starts_with(parent.value)
           and child.value[parent.value.size()] == '-';
  };
  // Collect channel stats per operator. Each operator gets its input from
  // the upstream channel and its output from the downstream channel.
  // Cross-boundary channels (between a parent and its sub-pipeline) are
  // attributed to the child operator, not the parent.
  for (auto const& prof : channel_profiles) {
    auto sep = prof.id.value.find(" -> ");
    TENZIR_ASSERT(sep != std::string::npos);
    auto sender = OpId{prof.id.value.substr(0, sep)};
    auto receiver = OpId{prof.id.value.substr(sep + 4)};
    // Skip the "_" side of boundary channels, and skip the parent side of
    // cross-boundary channels (parent <-> sub-pipeline child).
    auto skip_sender = sender.value == "_" or is_child_of(receiver, sender);
    auto skip_receiver = receiver.value == "_" or is_child_of(sender, receiver);
    auto bytes_in = prof.stats->in.bytes.load(std::memory_order::relaxed);
    auto bytes_out = prof.stats->out.bytes.load(std::memory_order::relaxed);
    auto batches_in = prof.stats->in.batches.load(std::memory_order::relaxed);
    auto batches_out = prof.stats->out.batches.load(std::memory_order::relaxed);
    auto events_in = prof.stats->in.events.load(std::memory_order::relaxed);
    auto events_out = prof.stats->out.events.load(std::memory_order::relaxed);
    auto signals_in = prof.stats->in.signals.load(std::memory_order::relaxed);
    auto signals_out = prof.stats->out.signals.load(std::memory_order::relaxed);
    auto capacity = prof.stats->capacity.load(std::memory_order::relaxed);
    auto clamp_sub = [](size_t a, size_t b) {
      return a >= b ? a - b : 0;
    };
    // Channel "in" = data pushed by sender = sender's output.
    if (not skip_sender) {
      auto& s = ops[sender];
      set(s.bytes_out, bytes_in);
      set(s.batches_out, batches_in);
      set(s.events_out, events_in);
      set(s.signals_out, signals_in);
    }
    // Channel "out" = data pulled by receiver = receiver's input.
    if (not skip_receiver) {
      auto& r = ops[receiver];
      set(r.bytes_in, bytes_out);
      set(r.batches_in, batches_out);
      set(r.events_in, events_out);
      set(r.signals_in, signals_out);
      set(r.input_bytes, clamp_sub(bytes_in, bytes_out));
      set(r.input_capacity, capacity);
    }
  }
  // Collect executor stats per operator.
  for (auto const& ex : executor_profiles) {
    auto& s = ops[ex.id];
    s.name = ex.name;
    set(s.cpu_ns, ex.stats->cpu_ns.load(std::memory_order::relaxed));
    set(s.wall_ns, ex.stats->wall_ns.load(std::memory_order::relaxed));
    set(s.task_count, ex.stats->task_count.load(std::memory_order::relaxed));
  }
  // Build operator entries with deltas against the previous snapshot.
  auto result = ProfilerSnapshot{};
  result.timestamp = timestamp;
  // Report the window alongside the counters, so that consumers derive rates
  // from the time that actually passed instead of assuming the nominal
  // sampling interval. This must stay the same value that divides `cpu` below.
  result.duration = elapsed;
  auto elapsed_ns
    = std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
  auto get = []<class T>(Option<T> const& field) -> T {
    return field.unwrap_or_default();
  };
  auto delta = [](size_t cur, size_t prev) -> uint64_t {
    return static_cast<uint64_t>(cur >= prev ? cur - prev : cur);
  };
  // The same rule for the CPU counter, which is signed. A cumulative counter
  // that decreased means the executor of this operator was reaped and a new one
  // started over from zero, so the current value is what accrued since the
  // restart. Subtracting the stale baseline instead would report the operator
  // as having consumed a negative amount of CPU time.
  auto delta_cpu = [](int64_t cur, int64_t prev) -> int64_t {
    return cur >= prev ? cur - prev : cur;
  };
  for (auto const& [id, s] : ops) {
    // A missing executor profile means that the operator has terminated: its
    // `ExecutorStats` was reported one final time and then reaped. Channels
    // adjacent to it can still be draining and thus keep recreating an entry
    // in `ops`, but all of its counters are frozen at that point, so there is
    // nothing left to report. Emitting anyway would diff the absent CPU time
    // against the last known total and yield a negative `cpu`.
    if (s.cpu_ns.is_none()) {
      continue;
    }
    auto cur = OpSnapshot{
      get(s.bytes_in),    get(s.bytes_out),   get(s.batches_in),
      get(s.batches_out), get(s.events_in),   get(s.events_out),
      get(s.signals_in),  get(s.signals_out), get(s.cpu_ns),
      get(s.wall_ns),     get(s.task_count),
    };
    auto& old = prev[id];
    // Express the CPU time consumed since the previous snapshot as a
    // percentage of the time that actually passed since then. This is a
    // percentage of a single core, so an operator running with multiple
    // parallel instances can exceed 100.
    auto cpu_usage = 0.0;
    if (elapsed_ns > 0) {
      auto delta_cpu_ns = delta_cpu(get(s.cpu_ns), old.cpu_ns);
      cpu_usage = static_cast<double>(delta_cpu_ns)
                  / static_cast<double>(elapsed_ns) * 100.0;
    }
    result.operators.push_back(OperatorProfileEntry{
      .operator_id = id.value,
      .name = s.name,
      .input_bytes = static_cast<uint64_t>(get(s.input_bytes)),
      .input_capacity = static_cast<uint64_t>(get(s.input_capacity)),
      .cpu = cpu_usage,
      .task_count = delta(get(s.task_count), old.task_count),
      .bytes_in = delta(get(s.bytes_in), old.bytes_in),
      .bytes_out = delta(get(s.bytes_out), old.bytes_out),
      .batches_in = delta(get(s.batches_in), old.batches_in),
      .batches_out = delta(get(s.batches_out), old.batches_out),
      .events_in = delta(get(s.events_in), old.events_in),
      .events_out = delta(get(s.events_out), old.events_out),
      .signals_in = delta(get(s.signals_in), old.signals_in),
      .signals_out = delta(get(s.signals_out), old.signals_out),
    });
    old = cur;
  }
  // Remove entries for operators that we did not emit above, either because
  // they are gone entirely or because their executor was already reaped.
  std::erase_if(prev, [&](auto const& kv) {
    auto it = ops.find(kv.first);
    return it == ops.end() or it->second.cpu_ns.is_none();
  });
  return result;
}

} // namespace tenzir
