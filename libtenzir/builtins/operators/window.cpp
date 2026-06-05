//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/multi_series.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/view3.hpp>

#include <arrow/compute/api.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/UnboundedQueue.h>

#include <chrono>
#include <map>
#include <set>
#include <vector>

namespace tenzir::plugins::window {

namespace {

using std::chrono::steady_clock;

struct WindowArgs {
  duration size = {};
  ast::expression on;
  Option<duration> every;
  duration tolerance = {};
  Option<duration> idle_timeout;
  located<ir::pipeline> pipe;
  let_id let;
};

/// Copies the given `rows` out of `input` into a new table slice. Identical to
/// the helper in the `group` operator.
auto take_rows(table_slice const& input, std::vector<int64_t> const& rows)
  -> table_slice {
  TENZIR_ASSERT(not rows.empty());
  auto builder = arrow::Int64Builder{arrow_memory_pool()};
  check(builder.Reserve(detail::narrow<int64_t>(rows.size())));
  for (auto row : rows) {
    check(builder.Append(row));
  }
  auto indices = finish(builder);
  auto datum = check(arrow::compute::Take(to_record_batch(input), indices));
  TENZIR_ASSERT(datum.kind() == arrow::Datum::Kind::RECORD_BATCH);
  auto result = table_slice{datum.record_batch(), input.schema()};
  result.offset(input.offset());
  result.import_time(input.import_time());
  return result;
}

/// Integer division rounding towards negative infinity (unlike C++ `/`, which
/// truncates towards zero). Correct for pre-epoch (negative) timestamps.
auto floor_div(int64_t a, int64_t b) -> int64_t {
  auto q = a / b;
  auto r = a % b;
  if (r != 0 and (r < 0) != (b < 0)) {
    --q;
  }
  return q;
}

struct TimerTick {
  steady_clock::time_point deadline;
};

struct WindowState {
  time end;
  steady_clock::time_point last_event;
};

/// The result of assigning a batch of rows to windows: which rows go into which
/// window (by window start), plus counts of dropped events and the largest
/// observed timestamp in the batch.
struct WindowAssignment {
  std::map<time, std::vector<int64_t>> groups;
  int64_t late_events = 0;
  int64_t invalid_events = 0;
  Option<time> batch_max;
};

class WindowBase {
public:
  explicit WindowBase(WindowArgs args) : args_{std::move(args)} {
  }

protected:
  auto start_impl(OpCtx& ctx) -> Task<void> {
    if (args_.idle_timeout) {
      // Background timer that wakes at the earliest idle deadline among the
      // open windows and signals `process_task` via the tick queue.
      ctx.spawn_task([frontier = frontier_queue_,
                      ticks = tick_queue_]() mutable -> Task<void> {
        auto deadline = co_await frontier->dequeue();
        while (true) {
          while (auto more = frontier->try_dequeue()) {
            deadline = std::min(deadline, *more);
          }
          co_await sleep_until(deadline);
          co_await ticks->enqueue(TimerTick{deadline});
          deadline = co_await frontier->dequeue();
        }
      });
      // After restoring from a checkpoint, windows may already be open. Their
      // wall-clock idle timestamps cannot be carried across the restart, so we
      // restart the idle clock from now and arm the timer.
      if (not open_.empty()) {
        auto now = steady_clock::now();
        for (auto& [start, state] : open_) {
          state.last_event = now;
        }
        arm_timer();
      }
    }
    co_return;
  }

  auto await_task_impl() const -> Task<Any> {
    if (not args_.idle_timeout) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await tick_queue_->dequeue();
  }

  auto process_impl(table_slice input, OpCtx& ctx) -> Task<void> {
    auto ts = eval(args_.on, input, ctx);
    auto pre_clock = current_time_;
    // Decide which windows each row belongs to. This is a pure computation over
    // the current state; the actual routing and clock advance happen below.
    auto assignment = assign_windows(ts, pre_clock);
    // Advance the monotonic event-time clock.
    if (assignment.batch_max) {
      current_time_ = pre_clock ? std::max(*pre_clock, *assignment.batch_max)
                                : *assignment.batch_max;
    }
    if (assignment.invalid_events > 0) {
      diagnostic::warning("`window` dropped {} event(s) where `on` did not "
                          "evaluate to a timestamp",
                          assignment.invalid_events)
        .primary(args_.on)
        .emit(ctx);
    }
    if (assignment.late_events > 0) {
      diagnostic::warning("`window` dropped {} late event(s) that arrived "
                          "after their window had closed",
                          assignment.late_events)
        .primary(args_.on)
        .emit(ctx);
    }
    // Route the grouped rows into their windows, spawning new subpipelines as
    // needed.
    auto now = steady_clock::now();
    for (auto& [start, group_rows] : assignment.groups) {
      auto end = start + args_.size;
      auto sub_slice = take_rows(input, group_rows);
      auto sub = ctx.get_sub(make_view(data{start}));
      if (not sub) {
        if (open_.contains(start)) {
          // The subpipeline terminated on its own; drop the stale entry and the
          // events destined for it.
          open_.erase(start);
          continue;
        }
        auto rec = record{};
        rec.emplace("start", data{start});
        rec.emplace("end", data{end});
        auto env = substitute_ctx::env_t{};
        env[args_.let] = ast::constant::kind{std::move(rec)};
        auto copy = args_.pipe.inner;
        if (not copy.substitute(substitute_ctx{ctx, &env}, true)) {
          continue;
        }
        seen_.insert(start);
        open_.emplace(start, WindowState{end, now});
        sub = co_await ctx.spawn_sub(data{start}, std::move(copy),
                                     tag_v<table_slice>);
      }
      TENZIR_ASSERT(sub);
      if (auto it = open_.find(start); it != open_.end()) {
        it->second.last_event = now;
      }
      std::ignore
        = co_await as<SubHandle<table_slice>>(*sub).push(std::move(sub_slice));
    }
    co_await close_passed_windows(ctx);
    prune_seen();
    // Arm the idle timer when transitioning from no open windows to some. While
    // windows stay open we let the timer re-arm itself on each tick instead of
    // enqueueing on every batch, which keeps the frontier queue small under
    // high throughput.
    if (args_.idle_timeout and timer_idle_ and not open_.empty()) {
      arm_timer();
    }
  }

  auto process_task_impl(Any result, OpCtx& ctx) -> Task<void> {
    // `process_task` only fires for the idle timer, which enqueues `TimerTick`s.
    std::ignore = result.as<TimerTick>();
    auto now = steady_clock::now();
    auto to_close = std::vector<time>{};
    for (const auto& [start, state] : open_) {
      if (state.last_event + *args_.idle_timeout <= now) {
        to_close.push_back(start);
      }
    }
    for (auto start : to_close) {
      co_await close_window(ctx, start);
    }
    // The timer consumed its deadline and will block on the frontier queue next,
    // so re-arm it for the remaining windows (or mark it idle if none remain).
    if (open_.empty()) {
      timer_idle_ = true;
    } else {
      arm_timer();
    }
  }

  auto snapshot_impl(Serde& serde) -> void {
    // The spawned window subpipelines are checkpointed and restored by the
    // executor; we only persist the bookkeeping needed to keep routing,
    // late-event detection, and closing consistent with them. Two pieces of
    // state are intentionally *not* preserved: the per-window wall-clock idle
    // timestamps (`steady_clock` is not meaningful across a restart, so the
    // idle clock restarts in `start`) and the in-memory queue/timer state
    // (re-created in `start`).
    serde("current_time", current_time_);
    serde("seen", seen_);
    // The open windows are (de)serialized through a plain vector of their start
    // times. We cannot serialize `open_` directly because `WindowState` holds a
    // wall-clock idle timestamp that is not meaningful across a restart; the
    // `end` is also redundant (always `start + size`). The reconciliation below
    // is a no-op when serializing (the vector mirrors the live state) and
    // rebuilds `open_` when deserializing.
    auto open_starts = std::vector<time>{};
    open_starts.reserve(open_.size());
    for (const auto& [start, state] : open_) {
      open_starts.push_back(start);
    }
    serde("open_starts", open_starts);
    auto now = steady_clock::now();
    auto rebuilt = std::map<time, WindowState>{};
    for (const auto& start : open_starts) {
      auto it = open_.find(start);
      auto last_event = it != open_.end() ? it->second.last_event : now;
      rebuilt.emplace(start, WindowState{start + args_.size, last_event});
    }
    open_ = std::move(rebuilt);
  }

private:
  /// Assigns each row of `ts` (the evaluated `on` expression) to the windows
  /// that contain its event time. A row may land in several windows when
  /// windows overlap (hopping). Rows whose timestamp is null or not a `time`
  /// are counted as invalid; rows whose only target windows have already closed
  /// are counted as late. This function does not mutate operator state.
  auto assign_windows(const multi_series& ts, Option<time> pre_clock) const
    -> WindowAssignment {
    auto result = WindowAssignment{};
    auto rows = ts.length();
    // The clock advances per event in stream order. Late-ness is therefore
    // determined by the events that precede an event in the stream, not by how
    // events happen to be grouped into batches.
    auto clock = pre_clock;
    for (auto row = int64_t{0}; row < rows; ++row) {
      auto value = materialize(ts.view3_at(row));
      auto t = try_as<time>(&value);
      if (not t) {
        result.invalid_events += 1;
        continue;
      }
      result.batch_max
        = result.batch_max ? std::max(*result.batch_max, *t) : *t;
      auto any_open = false;
      auto any_late = false;
      auto [first, last] = window_bounds(*t);
      for (auto index = first; index <= last; ++index) {
        auto start = window_start(index);
        auto end = start + args_.size;
        // A window has closed once the clock moved past `end + tolerance`. The
        // clock reflects all preceding events, so an out-of-order event is late
        // exactly when an earlier event already advanced past its window.
        if (clock and *clock > end + args_.tolerance) {
          any_late = true;
          continue;
        }
        if (open_.contains(start)) {
          result.groups[start].push_back(row);
          any_open = true;
          continue;
        }
        // The window has been seen before but is no longer open: its
        // subpipeline already finished (e.g. via `head`). Drop the event.
        if (seen_.contains(start)) {
          any_late = true;
          continue;
        }
        result.groups[start].push_back(row);
        any_open = true;
      }
      // Advance the clock with this event before processing the next one.
      clock = clock ? std::max(*clock, *t) : *t;
      if (not any_open and any_late) {
        result.late_events += 1;
      }
    }
    return result;
  }

  /// The window stride: `every` if given, otherwise `size` (tumbling).
  auto every() const -> duration {
    return args_.every ? *args_.every : args_.size;
  }

  /// Returns the inclusive `[first, last]` range of window indices (multiples of
  /// `every`) whose `[start, start + size)` interval contains `t`. The interval
  /// is right-open, so an event exactly on a boundary belongs to the next
  /// window. The computation is correct for pre-epoch (negative) timestamps.
  auto window_bounds(time t) const -> std::pair<int64_t, int64_t> {
    auto t_ns = t.time_since_epoch().count();
    auto every_ns = every().count();
    auto size_ns = args_.size.count();
    // Largest window start <= t.
    auto last = floor_div(t_ns, every_ns);
    // Smallest window start S with S + size > t.
    auto first = floor_div(t_ns - size_ns, every_ns) + 1;
    return {first, last};
  }

  /// Returns the start time of the window with the given index.
  auto window_start(int64_t index) const -> time {
    return time{} + duration{index * every().count()};
  }

  auto close_window(OpCtx& ctx, time start) -> Task<void> {
    open_.erase(start);
    if (auto sub = ctx.get_sub(make_view(data{start}))) {
      co_await as<SubHandle<table_slice>>(*sub).close();
    }
  }

  auto close_passed_windows(OpCtx& ctx) -> Task<void> {
    if (not current_time_) {
      co_return;
    }
    auto to_close = std::vector<time>{};
    // `open_` is ordered by start; since `end = start + size` with a constant
    // `size`, it is also ordered by end. Hence the first window that is not yet
    // passed marks the end of the closeable prefix.
    for (const auto& [start, state] : open_) {
      if (*current_time_ > state.end + args_.tolerance) {
        to_close.push_back(start);
      } else {
        break;
      }
    }
    for (auto start : to_close) {
      co_await close_window(ctx, start);
    }
  }

  /// Drops entries from `seen_` whose window has been passed by the clock. Late
  /// events for such windows are already detected against the clock, so the
  /// bookkeeping is no longer needed.
  auto prune_seen() -> void {
    if (not current_time_) {
      return;
    }
    while (not seen_.empty()) {
      auto start = *seen_.begin();
      auto end = start + args_.size;
      if (*current_time_ > end + args_.tolerance) {
        seen_.erase(seen_.begin());
      } else {
        break;
      }
    }
  }

  /// Schedules the next idle wake-up at the earliest deadline among the open
  /// windows. Precondition: `idle_timeout` is set and `open_` is not empty.
  auto arm_timer() -> void {
    auto earliest = steady_clock::time_point::max();
    for (const auto& [start, state] : open_) {
      earliest = std::min(earliest, state.last_event + *args_.idle_timeout);
    }
    frontier_queue_->enqueue(earliest);
    timer_idle_ = false;
  }

  using FrontierQueue = folly::coro::UnboundedQueue<steady_clock::time_point>;
  using TickQueue = folly::coro::BoundedQueue<TimerTick>;

  WindowArgs args_;

  /// Currently open windows, keyed and ordered by window start.
  std::map<time, WindowState> open_;
  /// Window starts that have been spawned at some point. Used to avoid
  /// re-opening a window whose subpipeline already finished.
  std::set<time> seen_;
  /// The largest event time observed so far. Moves forward only.
  Option<time> current_time_;
  /// Whether the idle timer is currently waiting to be armed (no pending
  /// wake-up scheduled). Only meaningful when `idle_timeout` is set.
  bool timer_idle_ = true;
  Arc<FrontierQueue> frontier_queue_{std::in_place};
  mutable Arc<TickQueue> tick_queue_{std::in_place, 1};
};

template <class Output>
class Window;

template <>
class Window<table_slice> final : public Operator<table_slice, table_slice>,
                                  private WindowBase {
public:
  explicit Window(WindowArgs args) : WindowBase{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return start_impl(ctx);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    return await_task_impl();
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    co_await process_impl(std::move(input), ctx);
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    return process_task_impl(std::move(result), ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
  }
};

template <>
class Window<void> final : public Operator<table_slice, void>,
                           private WindowBase {
public:
  explicit Window(WindowArgs args) : WindowBase{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    return start_impl(ctx);
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    return await_task_impl();
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    co_await process_impl(std::move(input), ctx);
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    return process_task_impl(std::move(result), ctx);
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
  }
};

class window_plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "window";
  }

  auto describe() const -> Description override {
    auto d = Describer<WindowArgs>{};
    auto size = d.named("size", &WindowArgs::size, "duration");
    d.named("on", &WindowArgs::on, "time");
    auto every = d.named("every", &WindowArgs::every, "duration");
    auto tolerance
      = d.named_optional("tolerance", &WindowArgs::tolerance, "duration");
    auto idle = d.named("idle_timeout", &WindowArgs::idle_timeout, "duration");
    auto pipe = d.pipeline(&WindowArgs::pipe, SubOptimize::from_downstream,
                           {{"window", &WindowArgs::let}});
    d.validate([size, every, tolerance, idle](DescribeCtx& ctx) -> Empty {
      auto sz = ctx.get(size);
      if (sz and *sz <= duration::zero()) {
        diagnostic::error("`size` must be a positive duration")
          .primary(ctx.get_location(size).value())
          .emit(ctx);
      }
      if (auto e = ctx.get(every)) {
        if (*e <= duration::zero()) {
          diagnostic::error("`every` must be a positive duration")
            .primary(ctx.get_location(every).value())
            .emit(ctx);
        } else if (sz and *e > *sz) {
          diagnostic::error("`every` must not be greater than `size`")
            .primary(ctx.get_location(every).value())
            .secondary(ctx.get_location(size).value())
            .emit(ctx);
        }
      }
      if (auto t = ctx.get(tolerance); t and *t < duration::zero()) {
        diagnostic::error("`tolerance` must not be negative")
          .primary(ctx.get_location(tolerance).value())
          .emit(ctx);
      }
      if (auto t = ctx.get(idle); t and *t <= duration::zero()) {
        diagnostic::error("`idle_timeout` must be a positive duration")
          .primary(ctx.get_location(idle).value())
          .emit(ctx);
      }
      return {};
    });
    d.spawner([pipe]<class Input>(DescribeCtx& ctx)
                -> failure_or<Option<SpawnWith<WindowArgs, Input>>> {
      if constexpr (std::same_as<Input, table_slice>) {
        TRY(auto p, ctx.get(pipe));
        TRY(auto output, p.inner.infer_type(tag_v<table_slice>, ctx));
        if (not output) {
          diagnostic::error("subpipeline must not produce bytes")
            .primary(p.source)
            .emit(ctx);
          return failure::promise();
        }
        return match(
          *output,
          [](tag<table_slice>)
            -> failure_or<Option<SpawnWith<WindowArgs, Input>>> {
            return [](WindowArgs args) {
              return Window<table_slice>{std::move(args)};
            };
          },
          [](tag<void>) -> failure_or<Option<SpawnWith<WindowArgs, Input>>> {
            return [](WindowArgs args) {
              return Window<void>{std::move(args)};
            };
          },
          [&](tag<chunk_ptr>)
            -> failure_or<Option<SpawnWith<WindowArgs, Input>>> {
            diagnostic::error("subpipeline must not produce bytes")
              .primary(p.source)
              .emit(ctx);
            return failure::promise();
          });
      } else {
        return {};
      }
    });
    return d.without_optimize();
  }
};

} // namespace

} // namespace tenzir::plugins::window

TENZIR_REGISTER_PLUGIN(tenzir::plugins::window::window_plugin)
