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
#include <tenzir/detail/saturating_arithmetic.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/multi_series.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/view3.hpp>

#include <arrow/compute/api.h>
#include <folly/coro/BoundedQueue.h>
#include <folly/coro/UnboundedQueue.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace tenzir::plugins::window {

namespace {

using std::chrono::steady_clock;
using std::chrono::system_clock;

enum class WindowBasis {
  time,
  count,
};

enum class WindowShape {
  tumbling,
  hopping,
  trailing,
};

enum class WindowClock {
  event_time,
  processing_time,
  sequence,
};

struct WindowArgs {
  located<data> size;
  Option<located<data>> every;
  bool trailing = false;
  Option<ast::expression> on;
  Option<ast::expression> trigger;
  duration tolerance = {};
  Option<duration> idle_timeout;
  located<ir::pipeline> pipe;
  let_id let;
};

struct WindowConfig {
  WindowBasis basis = WindowBasis::time;
  WindowShape shape = WindowShape::tumbling;
  WindowClock clock = WindowClock::event_time;
  duration time_size = {};
  duration time_stride = {};
  uint64_t count_size = 0;
  uint64_t count_stride = 0;
};

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

auto wall_now() -> time {
  return std::chrono::time_point_cast<duration>(system_clock::now());
}

auto as_count(located<data> const& value) -> Option<uint64_t> {
  if (auto count = try_as<uint64_t>(&value.inner)) {
    return *count;
  }
  if (auto count = try_as<int64_t>(&value.inner); count and *count >= 0) {
    return detail::narrow<uint64_t>(*count);
  }
  return None{};
}

auto resolve_config(WindowArgs const& args) -> WindowConfig {
  auto result = WindowConfig{};
  auto time_size = try_as<duration>(&args.size.inner);
  auto count_size = as_count(args.size);
  TENZIR_ASSERT(time_size or count_size);
  if (time_size) {
    result.basis = WindowBasis::time;
    result.clock
      = args.on ? WindowClock::event_time : WindowClock::processing_time;
    result.time_size = *time_size;
    result.time_stride = *time_size;
    if (args.trailing) {
      result.shape = WindowShape::trailing;
      if (args.every) {
        result.time_stride = *try_as<duration>(&args.every->inner);
      }
    } else if (args.every) {
      result.shape = WindowShape::hopping;
      result.time_stride = *try_as<duration>(&args.every->inner);
    }
    return result;
  }
  result.basis = WindowBasis::count;
  result.clock = WindowClock::sequence;
  result.count_size = *count_size;
  result.count_stride = *count_size;
  if (args.trailing) {
    result.shape = WindowShape::trailing;
    if (args.every) {
      result.count_stride = *as_count(*args.every);
    }
  } else if (args.every) {
    result.shape = WindowShape::hopping;
    result.count_stride = *as_count(*args.every);
  }
  return result;
}

struct TimerTick {
  steady_clock::time_point deadline;
};

struct TimeWindowState {
  time end;
  steady_clock::time_point last_event;
};

struct CountWindowState {
  uint64_t finish;
};

struct TrailingReorderEntry {
  table_slice row;
  bool trigger_matches = false;

  friend auto inspect(auto& f, TrailingReorderEntry& x) -> bool {
    return f.object(x).fields(f.field("row", x.row),
                              f.field("trigger_matches", x.trigger_matches));
  }
};

/// The result of assigning a batch of rows to event-time windows: which rows go
/// into which window (by window start), plus counts of dropped events and the
/// largest observed timestamp in the batch.
struct TimeWindowAssignment {
  std::map<time, std::vector<int64_t>> groups;
  int64_t late_events = 0;
  int64_t invalid_events = 0;
  Option<time> batch_max;
};

class WindowBase {
public:
  explicit WindowBase(WindowArgs args)
    : args_{std::move(args)}, config_{resolve_config(args_)} {
  }

protected:
  auto is_trailing() const -> bool {
    return config_.shape == WindowShape::trailing;
  }

  auto trailing_sequence() const -> uint64_t {
    return trailing_sequence_;
  }

  auto start_impl(OpCtx& ctx) -> Task<void> {
    if (not needs_timer()) {
      co_return;
    }
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
    if (config_.clock == WindowClock::event_time) {
      auto now = steady_clock::now();
      for (auto& [start, state] : open_time_) {
        state.last_event = now;
      }
    } else {
      co_await close_processing_time_windows(ctx, wall_now());
      prune_time_seen(wall_now());
    }
    if (not open_time_.empty()) {
      arm_timer();
    }
  }

  auto await_task_impl() const -> Task<Any> {
    if (not needs_timer()) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await tick_queue_->dequeue();
  }

  auto process_impl(table_slice input, OpCtx& ctx) -> Task<void> {
    if (config_.shape == WindowShape::trailing) {
      if (config_.basis == WindowBasis::time) {
        co_await process_trailing_time(std::move(input), ctx);
      } else {
        co_await process_trailing_count(std::move(input), ctx);
      }
      co_return;
    }
    if (config_.basis == WindowBasis::count) {
      co_await process_fixed_count(std::move(input), ctx);
      co_return;
    }
    if (config_.clock == WindowClock::event_time) {
      co_await process_fixed_event_time(std::move(input), ctx);
    } else {
      co_await process_fixed_processing_time(std::move(input), ctx);
    }
  }

  auto process_task_impl(Any result, OpCtx& ctx) -> Task<void> {
    std::ignore = result.as<TimerTick>();
    if (config_.clock == WindowClock::processing_time) {
      auto now = std::max(wall_now(), current_time_.unwrap_or(time::min()));
      current_time_ = now;
      co_await close_processing_time_windows(ctx, now);
      prune_time_seen(now);
    } else {
      auto now = steady_clock::now();
      auto to_close = std::vector<time>{};
      for (auto const& [start, state] : open_time_) {
        if (state.last_event + *args_.idle_timeout <= now) {
          to_close.push_back(start);
        }
      }
      for (auto start : to_close) {
        co_await close_time_window(ctx, start);
      }
    }
    if (open_time_.empty()) {
      timer_idle_ = true;
    } else {
      arm_timer();
    }
  }

  auto snapshot_impl(Serde& serde) -> void {
    serde("current_time", current_time_);
    serde("seen", seen_time_);
    auto open_starts = std::vector<time>{};
    open_starts.reserve(open_time_.size());
    for (auto const& [start, state] : open_time_) {
      open_starts.push_back(start);
    }
    serde("open_starts", open_starts);
    auto now = steady_clock::now();
    auto rebuilt_time = std::map<time, TimeWindowState>{};
    for (auto const& start : open_starts) {
      auto it = open_time_.find(start);
      auto last_event = it != open_time_.end() ? it->second.last_event : now;
      rebuilt_time.emplace(
        start, TimeWindowState{detail::saturating_add(start, config_.time_size),
                               last_event});
    }
    open_time_ = std::move(rebuilt_time);
    serde("sequence_offset", sequence_offset_);
    serde("seen_count", seen_count_);
    auto open_begins = std::vector<uint64_t>{};
    open_begins.reserve(open_count_.size());
    for (auto const& [begin, state] : open_count_) {
      open_begins.push_back(begin);
    }
    serde("open_begins", open_begins);
    auto rebuilt_count = std::map<uint64_t, CountWindowState>{};
    for (auto begin : open_begins) {
      rebuilt_count.emplace(begin,
                            CountWindowState{begin + config_.count_size});
    }
    open_count_ = std::move(rebuilt_count);
    serde("trailing_rows", trailing_rows_);
    serde("trailing_times", trailing_times_);
    serde("trailing_reorder", trailing_reorder_);
    serde("trailing_time_origin", trailing_time_origin_);
    serde("trailing_count_since_fire", trailing_count_since_fire_);
    serde("trailing_sequence", trailing_sequence_);
    serde("warned_trailing_cost", warned_trailing_cost_);
    serde("warned_trailing_children", warned_trailing_children_);
  }

private:
  auto needs_timer() const -> bool {
    if (config_.shape == WindowShape::trailing) {
      return false;
    }
    return config_.clock == WindowClock::processing_time
           or (config_.clock == WindowClock::event_time and args_.idle_timeout);
  }

  auto process_fixed_event_time(table_slice input, OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(args_.on);
    auto ts = eval(*args_.on, input, ctx);
    auto pre_clock = current_time_;
    auto assignment = assign_event_time_windows(ts, pre_clock);
    if (assignment.batch_max) {
      current_time_ = pre_clock ? std::max(*pre_clock, *assignment.batch_max)
                                : *assignment.batch_max;
    }
    if (assignment.invalid_events > 0) {
      diagnostic::warning("`window` dropped {} event(s) where `on` did not "
                          "evaluate to a timestamp",
                          assignment.invalid_events)
        .primary(*args_.on)
        .emit(ctx);
    }
    if (assignment.late_events > 0) {
      diagnostic::warning("`window` dropped {} late event(s) that arrived "
                          "after their window had closed",
                          assignment.late_events)
        .primary(*args_.on)
        .emit(ctx);
    }
    co_await route_time_groups(input, assignment.groups, ctx);
    co_await close_passed_event_time_windows(ctx);
    prune_event_time_seen();
    if (args_.idle_timeout and timer_idle_ and not open_time_.empty()) {
      arm_timer();
    }
  }

  auto process_fixed_processing_time(table_slice input, OpCtx& ctx)
    -> Task<void> {
    auto arrival = std::max(wall_now(), current_time_.unwrap_or(time::min()));
    current_time_ = arrival;
    co_await close_processing_time_windows(ctx, arrival);
    auto groups = std::map<time, std::vector<int64_t>>{};
    auto row_count = detail::narrow<int64_t>(input.rows());
    auto [first, last] = time_window_bounds(arrival);
    // Iterate inclusively without incrementing past `last`, which can be the
    // maximum index.
    auto span = static_cast<uint64_t>(last) - static_cast<uint64_t>(first);
    auto offset = uint64_t{0};
    do {
      auto index = static_cast<int64_t>(static_cast<uint64_t>(first) + offset);
      auto& rows = groups[time_window_start(index)];
      rows.reserve(input.rows());
      for (auto row = int64_t{0}; row < row_count; ++row) {
        rows.push_back(row);
      }
    } while (offset++ != span);
    co_await route_time_groups(input, groups, ctx);
    prune_time_seen(arrival);
    if (timer_idle_ and not open_time_.empty()) {
      arm_timer();
    }
  }

  auto process_fixed_count(table_slice input, OpCtx& ctx) -> Task<void> {
    auto row_count = detail::narrow<int64_t>(input.rows());
    for (auto row = int64_t{0}; row < row_count; ++row) {
      if (sequence_offset_ == std::numeric_limits<uint64_t>::max()) {
        diagnostic::error("`window` exhausted its event offset range").emit(ctx);
        co_return;
      }
      auto [first, last] = count_window_bounds(sequence_offset_);
      for (auto index = first; index <= last; ++index) {
        auto begin = index * config_.count_stride;
        if (not co_await route_count_row(input, row, begin, ctx)) {
          co_return;
        }
      }
      sequence_offset_ += 1;
      co_await close_passed_count_windows(ctx);
      prune_count_seen();
    }
  }

  auto process_trailing_time(table_slice input, OpCtx& ctx) -> Task<void> {
    auto timestamps = config_.clock == WindowClock::event_time
                        ? Option{eval(*args_.on, input, ctx)}
                        : None{};
    auto triggers
      = args_.trigger ? Option{eval(*args_.trigger, input, ctx)} : None{};
    auto invalid_events = int64_t{0};
    auto late_events = int64_t{0};
    auto invalid_triggers = int64_t{0};
    auto row_count = detail::narrow<int64_t>(input.rows());
    for (auto row = int64_t{0}; row < row_count; ++row) {
      auto event_time = wall_now();
      if (timestamps) {
        auto value = materialize(timestamps->view3_at(row));
        auto timestamp = try_as<time>(&value);
        if (not timestamp) {
          invalid_events += 1;
          continue;
        }
        event_time = *timestamp;
        auto reorder_cutoff
          = current_time_
              ? Option{detail::saturating_sub(*current_time_, args_.tolerance)}
              : None{};
        if (reorder_cutoff and event_time < *reorder_cutoff) {
          late_events += 1;
          continue;
        }
        current_time_
          = current_time_ ? std::max(*current_time_, event_time) : event_time;
        auto trigger_matches
          = evaluate_trigger(triggers, row, invalid_triggers);
        trailing_reorder_.emplace(
          event_time,
          TrailingReorderEntry{subslice(input, row, row + 1), trigger_matches});
        warn_about_trailing_cost(ctx);
        auto cutoff = detail::saturating_sub(*current_time_, args_.tolerance);
        co_await drain_trailing_reorder_buffer(cutoff, ctx);
        continue;
      }
      if (current_time_ and event_time < *current_time_) {
        event_time = *current_time_;
      }
      current_time_ = event_time;
      auto trigger_matches = evaluate_trigger(triggers, row, invalid_triggers);
      co_await process_trailing_time_row(subslice(input, row, row + 1),
                                         event_time, trigger_matches, ctx);
    }
    warn_about_invalid_triggers(invalid_triggers, ctx);
    if (invalid_events > 0) {
      diagnostic::warning("`window` dropped {} event(s) where `on` did not "
                          "evaluate to a timestamp",
                          invalid_events)
        .primary(*args_.on)
        .emit(ctx);
    }
    warn_about_late_events(late_events, ctx);
  }

  auto drain_trailing_reorder_buffer(Option<time> cutoff, OpCtx& ctx)
    -> Task<void> {
    while (not trailing_reorder_.empty()
           and (not cutoff or trailing_reorder_.begin()->first <= *cutoff)) {
      auto entry = trailing_reorder_.extract(trailing_reorder_.begin());
      auto event_time = entry.key();
      auto row = std::move(entry.mapped().row);
      auto trigger_matches = entry.mapped().trigger_matches;
      co_await process_trailing_time_row(std::move(row), event_time,
                                         trigger_matches, ctx);
    }
  }

  auto process_trailing_time_row(table_slice row, time event_time,
                                 bool trigger_matches, OpCtx& ctx)
    -> Task<void> {
    auto cutoff = detail::saturating_sub(event_time, config_.time_size);
    while (not trailing_times_.empty() and trailing_times_.front() < cutoff) {
      trailing_times_.pop_front();
      trailing_rows_.pop_front();
    }
    trailing_times_.push_back(event_time);
    trailing_rows_.push_back(row);
    warn_about_trailing_cost(ctx);
    if (not should_fire_trailing_time(event_time, trigger_matches)) {
      co_return;
    }
    auto records = multi_series{series{std::move(row)}};
    auto event = materialize(records.view3_at(0));
    auto event_record = try_as<record>(&event);
    TENZIR_ASSERT(event_record);
    auto window = record{};
    window.emplace("start", data{cutoff});
    window.emplace("end", data{event_time});
    window.emplace("event", data{std::move(*event_record)});
    co_await run_trailing_window(std::move(window), ctx);
  }

  auto warn_about_late_events(int64_t late_events, OpCtx& ctx) -> void {
    if (late_events == 0) {
      return;
    }
    diagnostic::warning("`window` dropped {} late event(s) that arrived "
                        "after their window had closed",
                        late_events)
      .primary(*args_.on)
      .emit(ctx);
  }

  auto process_trailing_count(table_slice input, OpCtx& ctx) -> Task<void> {
    auto triggers
      = args_.trigger ? Option{eval(*args_.trigger, input, ctx)} : None{};
    auto records = multi_series{series{input}};
    auto invalid_triggers = int64_t{0};
    auto row_count = detail::narrow<int64_t>(input.rows());
    for (auto row = int64_t{0}; row < row_count; ++row) {
      if (sequence_offset_ == std::numeric_limits<uint64_t>::max()) {
        diagnostic::error("`window` exhausted its event offset range").emit(ctx);
        co_return;
      }
      trailing_rows_.push_back(subslice(input, row, row + 1));
      if (trailing_rows_.size() > config_.count_size) {
        trailing_rows_.pop_front();
      }
      warn_about_trailing_cost(ctx);
      auto trigger_matches = evaluate_trigger(triggers, row, invalid_triggers);
      if (not should_fire_trailing_count(trigger_matches)) {
        sequence_offset_ += 1;
        continue;
      }
      auto event = materialize(records.view3_at(row));
      auto event_record = try_as<record>(&event);
      TENZIR_ASSERT(event_record);
      auto finish = sequence_offset_ + 1;
      auto begin
        = finish > config_.count_size ? finish - config_.count_size : 0;
      auto window = record{};
      window.emplace("begin", data{begin});
      window.emplace("finish", data{finish});
      window.emplace("event", data{std::move(*event_record)});
      co_await run_trailing_window(std::move(window), ctx);
      sequence_offset_ += 1;
    }
    warn_about_invalid_triggers(invalid_triggers, ctx);
  }

  /// Applies a duration cadence to trailing-window candidates. The first event
  /// starts the cadence. Once it elapses, the next event whose trigger matches
  /// fires the window and starts the next cadence.
  auto should_fire_trailing_time(time event_time, bool trigger_matches)
    -> bool {
    if (not args_.every) {
      return trigger_matches;
    }
    if (not trailing_time_origin_) {
      trailing_time_origin_ = event_time;
      return false;
    }
    auto origin = trailing_time_origin_->time_since_epoch();
    if (origin > duration::max() - config_.time_stride
        or event_time < *trailing_time_origin_ + config_.time_stride
        or not trigger_matches) {
      return false;
    }
    trailing_time_origin_ = event_time;
    return true;
  }

  /// Applies an event-count cadence to trailing-window candidates. Every input
  /// event advances the cadence. Once it elapses, the next event whose trigger
  /// matches fires the window and starts the next cadence.
  auto should_fire_trailing_count(bool trigger_matches) -> bool {
    if (not args_.every) {
      return trigger_matches;
    }
    if (trailing_count_since_fire_ < config_.count_stride) {
      trailing_count_since_fire_ += 1;
    }
    if (trailing_count_since_fire_ < config_.count_stride
        or not trigger_matches) {
      return false;
    }
    trailing_count_since_fire_ = 0;
    return true;
  }

  /// Decides whether the trailing window can fire for `row`. Without a
  /// `trigger` argument every event qualifies. Events whose trigger expression
  /// does not evaluate to `true` never qualify; non-boolean results are counted
  /// for a batched warning. All events enter retained history and advance an
  /// optional cadence regardless.
  auto evaluate_trigger(Option<multi_series> const& triggers, int64_t row,
                        int64_t& invalid_triggers) const -> bool {
    if (not triggers) {
      return true;
    }
    auto value = materialize(triggers->view3_at(row));
    auto flag = try_as<bool>(&value);
    if (not flag) {
      invalid_triggers += 1;
      return false;
    }
    return *flag;
  }

  auto warn_about_invalid_triggers(int64_t invalid_triggers, OpCtx& ctx)
    -> void {
    if (invalid_triggers == 0) {
      return;
    }
    diagnostic::warning("`window` did not fire for {} event(s) where "
                        "`trigger` did not evaluate to `bool`",
                        invalid_triggers)
      .primary(*args_.trigger)
      .emit(ctx);
  }

  auto run_trailing_window(record window, OpCtx& ctx) -> Task<void> {
    auto key = data{trailing_sequence_};
    auto copy = args_.pipe.inner;
    copy.bind(args_.let, ast::constant::kind{std::move(window)});
    auto sub
      = co_await ctx.plan_and_spawn_sub<table_slice>(key, std::move(copy));
    if (not sub) {
      // Do not advance the sequence: no child owns this key, and the ordered
      // output release requires the finished sequences to stay contiguous.
      co_return;
    }
    trailing_in_flight_ += 1;
    warn_about_trailing_children(ctx);
    auto& handle = as<SubHandle<table_slice>>(*sub);
    auto terminated = false;
    for (auto const& row : trailing_rows_) {
      if (auto result = co_await handle.push(row); result.is_err()) {
        terminated = true;
        break;
      }
    }
    if (not terminated) {
      co_await handle.close();
    }
    trailing_sequence_ += 1;
  }

  auto warn_about_trailing_cost(OpCtx& ctx) -> void {
    static constexpr auto warning_threshold = size_t{100'000};
    auto retained = trailing_rows_.size() + trailing_reorder_.size();
    if (warned_trailing_cost_ or retained < warning_threshold) {
      return;
    }
    diagnostic::warning("`window` retained {} events for trailing evaluation",
                        retained)
      .note("generic trailing windows replay all retained events for every "
            "window invocation")
      .emit(ctx);
    warned_trailing_cost_ = true;
  }

  auto warn_about_trailing_children(OpCtx& ctx) -> void {
    static constexpr auto warning_threshold = uint64_t{1'000};
    if (warned_trailing_children_ or trailing_in_flight_ < warning_threshold) {
      return;
    }
    diagnostic::warning("`window` has {} trailing subpipelines in flight",
                        trailing_in_flight_)
      .note("the subpipeline completes slower than events arrive; memory "
            "grows until it catches up")
      .emit(ctx);
    warned_trailing_children_ = true;
  }

protected:
  auto finalize_impl(OpCtx& ctx) -> Task<FinalizeBehavior> {
    if (config_.shape == WindowShape::trailing
        and config_.clock == WindowClock::event_time) {
      co_await drain_trailing_reorder_buffer(None{}, ctx);
    }
    co_return FinalizeBehavior::done;
  }

  /// Called by the operator variants when a trailing child completes. The
  /// in-flight count is derived from live children, so it is intentionally not
  /// serialized and may already be zero after a restore.
  auto on_trailing_child_finished() -> void {
    if (trailing_in_flight_ > 0) {
      trailing_in_flight_ -= 1;
    }
  }

private:
  /// Assigns each row of `ts` to the fixed event-time windows that contain it.
  /// The clock advances per event in stream order, independent of slice
  /// boundaries.
  auto assign_event_time_windows(multi_series const& ts,
                                 Option<time> pre_clock) const
    -> TimeWindowAssignment {
    auto result = TimeWindowAssignment{};
    auto clock = pre_clock;
    for (auto row = int64_t{0}; row < ts.length(); ++row) {
      auto value = materialize(ts.view3_at(row));
      auto timestamp = try_as<time>(&value);
      if (not timestamp) {
        result.invalid_events += 1;
        continue;
      }
      result.batch_max = result.batch_max
                           ? std::max(*result.batch_max, *timestamp)
                           : *timestamp;
      auto any_open = false;
      auto any_late = false;
      auto [first, last] = time_window_bounds(*timestamp);
      // Iterate inclusively without incrementing past `last`, which can be the
      // maximum index. `continue` re-evaluates the do-while condition.
      auto span = static_cast<uint64_t>(last) - static_cast<uint64_t>(first);
      auto offset = uint64_t{0};
      do {
        auto index
          = static_cast<int64_t>(static_cast<uint64_t>(first) + offset);
        auto start = time_window_start(index);
        auto end = detail::saturating_add(start, config_.time_size);
        if (clock and *clock >= detail::saturating_add(end, args_.tolerance)) {
          any_late = true;
          continue;
        }
        if (open_time_.contains(start)) {
          result.groups[start].push_back(row);
          any_open = true;
          continue;
        }
        if (seen_time_.contains(start)) {
          any_late = true;
          continue;
        }
        result.groups[start].push_back(row);
        any_open = true;
      } while (offset++ != span);
      clock = clock ? std::max(*clock, *timestamp) : *timestamp;
      if (not any_open and any_late) {
        result.late_events += 1;
      }
    }
    return result;
  }

  auto route_time_groups(table_slice const& input,
                         std::map<time, std::vector<int64_t>>& groups,
                         OpCtx& ctx) -> Task<void> {
    auto now = steady_clock::now();
    for (auto& [start, rows] : groups) {
      auto end = detail::saturating_add(start, config_.time_size);
      auto sub_slice = take_rows(input, rows);
      auto sub = ctx.get_sub(make_view(data{start}));
      if (not sub) {
        if (open_time_.contains(start)) {
          open_time_.erase(start);
          continue;
        }
        if (seen_time_.contains(start)) {
          continue;
        }
        auto window = record{};
        window.emplace("start", data{start});
        window.emplace("end", data{end});
        auto copy = args_.pipe.inner;
        copy.bind(args_.let, ast::constant::kind{std::move(window)});
        sub = co_await ctx.plan_and_spawn_sub<table_slice>(data{start},
                                                           std::move(copy));
        if (not sub) {
          continue;
        }
        seen_time_.insert(start);
        open_time_.emplace(start, TimeWindowState{end, now});
      }
      TENZIR_ASSERT(sub);
      if (auto it = open_time_.find(start); it != open_time_.end()) {
        it->second.last_event = now;
      }
      std::ignore
        = co_await as<SubHandle<table_slice>>(*sub).push(std::move(sub_slice));
    }
  }

  auto route_count_row(table_slice const& input, int64_t row, uint64_t begin,
                       OpCtx& ctx) -> Task<bool> {
    auto sub = ctx.get_sub(make_view(data{begin}));
    if (not sub) {
      if (open_count_.contains(begin)) {
        open_count_.erase(begin);
        co_return true;
      }
      if (seen_count_.contains(begin)) {
        co_return true;
      }
      if (begin > std::numeric_limits<uint64_t>::max() - config_.count_size) {
        diagnostic::error(
          "the count window's finish exceeds the event offset range")
          .primary(args_.size.source, "window begins at offset {} with size {}",
                   begin, config_.count_size)
          .emit(ctx);
        co_return false;
      }
      auto finish = begin + config_.count_size;
      auto window = record{};
      window.emplace("begin", data{begin});
      window.emplace("finish", data{finish});
      auto copy = args_.pipe.inner;
      copy.bind(args_.let, ast::constant::kind{std::move(window)});
      sub = co_await ctx.plan_and_spawn_sub<table_slice>(data{begin},
                                                         std::move(copy));
      if (not sub) {
        co_return true;
      }
      seen_count_.insert(begin);
      open_count_.emplace(begin, CountWindowState{finish});
    }
    TENZIR_ASSERT(sub);
    std::ignore = co_await as<SubHandle<table_slice>>(*sub).push(
      subslice(input, row, row + 1));
    co_return true;
  }

  /// The inclusive range of epoch-aligned fixed time-window indices that
  /// contain `timestamp`.
  auto time_window_bounds(time timestamp) const -> std::pair<int64_t, int64_t> {
    auto timestamp_ns = timestamp.time_since_epoch().count();
    auto stride_ns = config_.time_stride.count();
    auto size_ns = config_.time_size.count();
    auto last = floor_div(timestamp_ns, stride_ns);
    // Near the minimum timestamp, `timestamp_ns - size_ns` is not
    // representable. All indices below the representable range clamp to the
    // same minimal window start, so the earliest such index covers them.
    constexpr auto min_ns = std::numeric_limits<int64_t>::min();
    auto first = timestamp_ns < min_ns + size_ns
                   ? floor_div(min_ns, stride_ns)
                   : floor_div(timestamp_ns - size_ns, stride_ns) + 1;
    return {first, last};
  }

  auto time_window_start(int64_t index) const -> time {
    return time{}
           + duration{
             detail::saturating_mul(index, config_.time_stride.count())};
  }

  /// The inclusive range of fixed count-window indices that contain `offset`.
  /// Count windows start at offset zero, so no pre-stream windows are created.
  auto count_window_bounds(uint64_t offset) const
    -> std::pair<uint64_t, uint64_t> {
    auto last = offset / config_.count_stride;
    auto first = offset < config_.count_size
                   ? uint64_t{0}
                   : (offset - config_.count_size) / config_.count_stride + 1;
    return {first, last};
  }

  auto close_time_window(OpCtx& ctx, time start) -> Task<void> {
    open_time_.erase(start);
    if (auto sub = ctx.get_sub(make_view(data{start}))) {
      co_await as<SubHandle<table_slice>>(*sub).close();
    }
  }

  auto close_count_window(OpCtx& ctx, uint64_t begin) -> Task<void> {
    open_count_.erase(begin);
    if (auto sub = ctx.get_sub(make_view(data{begin}))) {
      co_await as<SubHandle<table_slice>>(*sub).close();
    }
  }

  auto close_passed_event_time_windows(OpCtx& ctx) -> Task<void> {
    if (not current_time_) {
      co_return;
    }
    auto to_close = std::vector<time>{};
    for (auto const& [start, state] : open_time_) {
      if (*current_time_
          >= detail::saturating_add(state.end, args_.tolerance)) {
        to_close.push_back(start);
      } else {
        break;
      }
    }
    for (auto start : to_close) {
      co_await close_time_window(ctx, start);
    }
  }

  auto close_processing_time_windows(OpCtx& ctx, time now) -> Task<void> {
    auto to_close = std::vector<time>{};
    for (auto const& [start, state] : open_time_) {
      if (now >= state.end) {
        to_close.push_back(start);
      } else {
        break;
      }
    }
    for (auto start : to_close) {
      co_await close_time_window(ctx, start);
    }
  }

  auto close_passed_count_windows(OpCtx& ctx) -> Task<void> {
    auto to_close = std::vector<uint64_t>{};
    for (auto const& [begin, state] : open_count_) {
      if (sequence_offset_ >= state.finish) {
        to_close.push_back(begin);
      } else {
        break;
      }
    }
    for (auto begin : to_close) {
      co_await close_count_window(ctx, begin);
    }
  }

  auto prune_event_time_seen() -> void {
    if (not current_time_) {
      return;
    }
    while (not seen_time_.empty()) {
      auto start = *seen_time_.begin();
      auto end = detail::saturating_add(start, config_.time_size);
      if (*current_time_ >= detail::saturating_add(end, args_.tolerance)) {
        seen_time_.erase(seen_time_.begin());
      } else {
        break;
      }
    }
  }

  auto prune_time_seen(time now) -> void {
    while (not seen_time_.empty()) {
      auto start = *seen_time_.begin();
      if (now >= detail::saturating_add(start, config_.time_size)) {
        seen_time_.erase(seen_time_.begin());
      } else {
        break;
      }
    }
  }

  auto prune_count_seen() -> void {
    while (not seen_count_.empty()) {
      auto begin = *seen_count_.begin();
      if (sequence_offset_ >= begin + config_.count_size) {
        seen_count_.erase(seen_count_.begin());
      } else {
        break;
      }
    }
  }

  auto arm_timer() -> void {
    TENZIR_ASSERT(not open_time_.empty());
    auto earliest = steady_clock::time_point::max();
    if (config_.clock == WindowClock::processing_time) {
      auto delay = detail::saturating_sub(
        open_time_.begin()->second.end.time_since_epoch(),
        wall_now().time_since_epoch());
      earliest = detail::saturating_add(steady_clock::now(),
                                        std::max(delay, duration::zero()));
    } else {
      TENZIR_ASSERT(args_.idle_timeout);
      for (auto const& [start, state] : open_time_) {
        earliest = std::min(earliest, detail::saturating_add(
                                        state.last_event, *args_.idle_timeout));
      }
    }
    frontier_queue_->enqueue(earliest);
    timer_idle_ = false;
  }

  using FrontierQueue = folly::coro::UnboundedQueue<steady_clock::time_point>;
  using TickQueue = folly::coro::BoundedQueue<TimerTick>;

  WindowArgs args_;
  WindowConfig config_;
  std::map<time, TimeWindowState> open_time_;
  std::set<time> seen_time_;
  Option<time> current_time_;
  std::map<uint64_t, CountWindowState> open_count_;
  std::set<uint64_t> seen_count_;
  uint64_t sequence_offset_ = 0;
  std::deque<table_slice> trailing_rows_;
  std::deque<time> trailing_times_;
  /// A multimap preserves arrival order among equal timestamps.
  std::multimap<time, TrailingReorderEntry> trailing_reorder_;
  Option<time> trailing_time_origin_;
  uint64_t trailing_count_since_fire_ = 0;
  uint64_t trailing_sequence_ = 0;
  uint64_t trailing_in_flight_ = 0;
  bool warned_trailing_cost_ = false;
  bool warned_trailing_children_ = false;
  bool timer_idle_ = true;
  Arc<FrontierQueue> frontier_queue_{std::in_place};
  mutable Arc<TickQueue> tick_queue_{std::in_place, 1};
};

struct TrailingOutputState {
  // `process_sub` may run concurrently. Keep the critical section limited to
  // moving Arrow-backed slices into the per-window output buffer.
  std::mutex mutex;
  std::map<uint64_t, std::vector<table_slice>> pending;
  std::set<uint64_t> finished;
  uint64_t next = 0;
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

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    return finalize_impl(ctx);
  }

  auto process_sub(SubKeyView key, table_slice slice, Push<table_slice>& push,
                   OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (not is_trailing()) {
      co_await push(std::move(slice));
      co_return;
    }
    auto key_data = materialize(key);
    auto sequence = try_as<uint64_t>(&key_data);
    TENZIR_ASSERT(sequence);
    auto guard = std::lock_guard{trailing_output_->mutex};
    if (*sequence < trailing_output_->next) {
      co_return;
    }
    trailing_output_->pending[*sequence].push_back(std::move(slice));
  }

  auto finish_sub(SubKeyView key, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (not is_trailing()) {
      co_return;
    }
    on_trailing_child_finished();
    auto key_data = materialize(key);
    auto sequence = try_as<uint64_t>(&key_data);
    TENZIR_ASSERT(sequence);
    auto ready = std::vector<table_slice>{};
    {
      auto guard = std::lock_guard{trailing_output_->mutex};
      if (*sequence < trailing_output_->next) {
        co_return;
      }
      trailing_output_->finished.insert(*sequence);
      while (trailing_output_->finished.contains(trailing_output_->next)) {
        auto it = trailing_output_->pending.find(trailing_output_->next);
        if (it != trailing_output_->pending.end()) {
          ready.insert(ready.end(), std::make_move_iterator(it->second.begin()),
                       std::make_move_iterator(it->second.end()));
          trailing_output_->pending.erase(it);
        }
        trailing_output_->finished.erase(trailing_output_->next);
        trailing_output_->next += 1;
      }
    }
    for (auto& slice : ready) {
      co_await push(std::move(slice));
    }
  }

  auto snapshot(Serde& serde) -> void override {
    snapshot_impl(serde);
    if (serde.is_loading()) {
      // Subpipelines do not survive a restore. Drop their partial output and
      // resume ordered release at the first sequence that has not been spawned.
      auto guard = std::lock_guard{trailing_output_->mutex};
      trailing_output_->pending.clear();
      trailing_output_->finished.clear();
      trailing_output_->next = trailing_sequence();
    }
  }

private:
  Arc<TrailingOutputState> trailing_output_{std::in_place};
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

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    return finalize_impl(ctx);
  }

  auto finish_sub(SubKeyView key, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key, ctx);
    if (is_trailing()) {
      on_trailing_child_finished();
    }
    co_return;
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
    auto size = d.named("size", &WindowArgs::size, "duration|uint");
    auto every = d.named("every", &WindowArgs::every, "duration|uint");
    auto trailing = d.named("trailing", &WindowArgs::trailing);
    auto on = d.named("on", &WindowArgs::on, "time");
    auto trigger = d.named("trigger", &WindowArgs::trigger, "bool");
    auto tolerance
      = d.named_optional("tolerance", &WindowArgs::tolerance, "duration");
    auto idle = d.named("idle_timeout", &WindowArgs::idle_timeout, "duration");
    auto pipe = d.pipeline(&WindowArgs::pipe, SubOptimize::from_downstream,
                           {{"window", &WindowArgs::let}});
    d.validate([size, every, trailing, on, trigger, tolerance,
                idle](DescribeCtx& ctx) -> Empty {
      auto raw_size = ctx.get(size);
      if (not raw_size) {
        return {};
      }
      auto time_size = try_as<duration>(&raw_size->inner);
      auto count_size = as_count(*raw_size);
      auto signed_size = try_as<int64_t>(&raw_size->inner);
      if (signed_size and *signed_size < 0) {
        diagnostic::error("`size` must be positive")
          .primary(raw_size->source)
          .emit(ctx);
        return {};
      }
      if (not time_size and not count_size) {
        diagnostic::error("`size` must be a duration or an unsigned integer")
          .primary(raw_size->source)
          .emit(ctx);
        return {};
      }
      if ((time_size and *time_size <= duration::zero())
          or (count_size and *count_size == 0)) {
        diagnostic::error("`size` must be positive")
          .primary(raw_size->source)
          .emit(ctx);
      }
      auto raw_every = ctx.get(every);
      auto is_trailing = ctx.get(trailing).value_or(false);
      if (raw_every) {
        auto every_time = try_as<duration>(&raw_every->inner);
        auto every_count = as_count(*raw_every);
        auto signed_every = try_as<int64_t>(&raw_every->inner);
        if (signed_every and *signed_every < 0) {
          diagnostic::error("`every` must be positive")
            .primary(raw_every->source)
            .emit(ctx);
        } else if (not every_time and not every_count) {
          diagnostic::error("`every` must be a duration or an unsigned integer")
            .primary(raw_every->source)
            .emit(ctx);
        } else if ((time_size and not every_time)
                   or (count_size and not every_count)) {
          diagnostic::error("`every` must have the same type as `size`")
            .primary(raw_every->source)
            .secondary(raw_size->source)
            .emit(ctx);
        } else if ((every_time and *every_time <= duration::zero())
                   or (every_count and *every_count == 0)) {
          diagnostic::error("`every` must be positive")
            .primary(raw_every->source)
            .emit(ctx);
        } else if (not is_trailing
                   and ((time_size and *every_time > *time_size)
                        or (count_size and *every_count > *count_size))) {
          diagnostic::error("`every` must not be greater than `size`")
            .primary(raw_every->source)
            .secondary(raw_size->source)
            .emit(ctx);
        }
      }
      auto on_value = ctx.get(on);
      if (ctx.get(trigger) and not is_trailing) {
        diagnostic::error("`trigger` is only valid for trailing windows")
          .primary(ctx.get_location(trigger).value())
          .hint("set `trailing=true` to run a trailing window")
          .emit(ctx);
      }
      auto tolerance_value = ctx.get(tolerance);
      auto idle_value = ctx.get(idle);
      if (count_size and on_value) {
        diagnostic::error("`on` is only valid for duration windows")
          .primary(ctx.get_location(on).value())
          .secondary(raw_size->source)
          .emit(ctx);
      }
      auto fixed_event_time = time_size and not is_trailing and on_value;
      auto trailing_event_time = time_size and is_trailing and on_value;
      if (tolerance_value and not fixed_event_time
          and not trailing_event_time) {
        diagnostic::error("`tolerance` requires a duration window with `on`")
          .primary(ctx.get_location(tolerance).value())
          .emit(ctx);
      } else if (tolerance_value and *tolerance_value < duration::zero()) {
        diagnostic::error("`tolerance` must not be negative")
          .primary(ctx.get_location(tolerance).value())
          .emit(ctx);
      }
      if (idle_value and not fixed_event_time) {
        diagnostic::error(
          "`idle_timeout` is only valid for fixed event-time windows")
          .primary(ctx.get_location(idle).value())
          .emit(ctx);
      } else if (idle_value and *idle_value <= duration::zero()) {
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
        return match(
          output,
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
