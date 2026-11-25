//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/channel.hpp>
#include <tenzir/async/notify.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/builtins/join/common.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/concatenate.hpp>
#include <tenzir/nova/event_builder.hpp>
#include <tenzir/nova/materialize.hpp>
#include <tenzir/nova/slice.hpp>

#include <folly/CancellationToken.h>
#include <folly/coro/Collect.h>
#include <folly/coro/WithCancellation.h>

#include <algorithm>
#include <chrono>
#include <deque>
#include <functional>
#include <limits>
#include <tuple>
#include <vector>

namespace tenzir::plugins::join {

namespace {

inline constexpr auto no_deadline = std::numeric_limits<int64_t>::max();
inline constexpr auto right_channel_capacity = size_t{16};

auto steady_now_ns() -> int64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
           std::chrono::steady_clock::now().time_since_epoch())
    .count();
}

auto estimate_events_bytes(nova::Events const& events) -> uint64_t {
  auto const data_bytes = static_cast<uint64_t>(events.data.approx_bytes());
  auto const meta_bytes = static_cast<uint64_t>(events.length())
                          * (sizeof(nova::Time) + sizeof(bool) + 16);
  return detail::saturating_add(data_bytes, meta_bytes);
}

template <class Batch>
struct BatchPool {
  struct Slot {
    Option<Batch> batch;
    uint32_t live_rows = 0;
    uint64_t approx_bytes = 0;
  };

  auto insert(Batch batch, uint32_t live_rows, uint64_t bytes) -> uint32_t {
    TENZIR_ASSERT_GT(live_rows, 0u);
    auto id = uint32_t{0};
    if (not free_ids.empty()) {
      id = free_ids.back();
      free_ids.pop_back();
      slots[id] = Slot{std::move(batch), live_rows, bytes};
    } else {
      id = detail::narrow<uint32_t>(slots.size());
      slots.push_back(Slot{std::move(batch), live_rows, bytes});
    }
    total_bytes = detail::saturating_add(total_bytes, bytes);
    return id;
  }

  auto retain_row(uint32_t id) -> void {
    auto& slot = slots[id];
    TENZIR_ASSERT_GT(slot.live_rows, 0u);
    ++slot.live_rows;
  }

  auto release_row(uint32_t id) -> void {
    auto& slot = slots[id];
    TENZIR_ASSERT_GT(slot.live_rows, 0u);
    if (--slot.live_rows == 0) {
      total_bytes = total_bytes >= slot.approx_bytes
                      ? total_bytes - slot.approx_bytes
                      : 0;
      slot.batch = None{};
      slot.approx_bytes = 0;
      free_ids.push_back(id);
    }
  }

  auto get(uint32_t id) const -> Batch const& {
    TENZIR_ASSERT(slots[id].batch);
    return *slots[id].batch;
  }

  auto clear() -> void {
    slots.clear();
    free_ids.clear();
    total_bytes = 0;
  }

  auto approx_bytes() const noexcept -> uint64_t {
    return detail::saturating_add(
      total_bytes,
      static_cast<uint64_t>(slots.size()) * sizeof(Slot)
        + static_cast<uint64_t>(free_ids.capacity()) * sizeof(uint32_t));
  }

  // `std::deque` keeps `Slot` addresses stable across growth so
  // `nova::EventsAssembler` references into `slots[id]` stay valid until flush.
  std::deque<Slot> slots;
  std::vector<uint32_t> free_ids;
  uint64_t total_bytes = 0;
};

struct Candidate {
  int64_t right_ts = 0;
  uint64_t right_seq = 0;
  uint32_t right_id = 0;
};

auto abs_diff_u64(int64_t a, int64_t b) noexcept -> uint64_t {
  return a >= b ? static_cast<uint64_t>(a) - static_cast<uint64_t>(b)
                : static_cast<uint64_t>(b) - static_cast<uint64_t>(a);
}

auto is_better_candidate(PickMode pick, int64_t left_ts, Candidate const& cand,
                         Candidate const& best) noexcept -> bool {
  switch (pick) {
    case PickMode::all:
      return false;
    case PickMode::latest:
      return cand.right_ts > best.right_ts
             or (cand.right_ts == best.right_ts
                 and cand.right_seq > best.right_seq);
    case PickMode::earliest:
      return cand.right_ts < best.right_ts
             or (cand.right_ts == best.right_ts
                 and cand.right_seq < best.right_seq);
    case PickMode::nearest: {
      auto const cand_dist = abs_diff_u64(cand.right_ts, left_ts);
      auto const best_dist = abs_diff_u64(best.right_ts, left_ts);
      if (cand_dist != best_dist) {
        return cand_dist < best_dist;
      }
      auto const cand_past = cand.right_ts <= left_ts;
      auto const best_past = best.right_ts <= left_ts;
      if (cand_past != best_past) {
        return cand_past;
      }
      return cand.right_seq > best.right_seq;
    }
  }
  TENZIR_UNREACHABLE();
}

auto extract_timestamps(nova::Evaluator& eval, nova::Events const& events,
                        location ts_loc, bool& warned_invalid,
                        diagnostic_handler& dh)
  -> std::vector<Option<int64_t>> {
  auto ctx = nova::EvalCtx{dh};
  auto col = eval.eval(events, ctx);
  auto values = std::vector<Option<int64_t>>(
    detail::narrow<size_t>(events.length()), None{});
  auto warn_once = [&](std::string_view detail_msg) {
    if (not warned_invalid) {
      warned_invalid = true;
      diagnostic::warning("{}", detail_msg)
        .primary(ts_loc)
        .note("events without a valid `time` timestamp cannot match in "
              "windowed `join`")
        .emit(dh);
    }
  };
  match(
    col,
    [&]<nova::data_type Tag>(nova::Array<Tag> const& arr) {
      if constexpr (std::same_as<Tag, nova::Time>) {
        for (auto row : nova::storage::true_bits(events.mask)) {
          values[detail::narrow<size_t>(row)]
            = (*arr.get(row)).time_since_epoch().count();
        }
      } else if constexpr (std::same_as<Tag, nova::Null>) {
        if (events.mask.any()) {
          warn_once("missing `on` timestamp value in `join`");
        }
      } else {
        if (events.mask.any()) {
          warn_once("expected `time` for `on` timestamp in `join`");
        }
      }
    },
    [&](nova::UnionArray const&) {
      for (auto row : nova::storage::true_bits(events.mask)) {
        auto view = col.get(row);
        if (auto const* t = try_as<nova::RowView<nova::Time>>(view)) {
          values[detail::narrow<size_t>(row)]
            = (**t).time_since_epoch().count();
        } else if (is<nova::RowView<nova::Null>>(view)) {
          warn_once("missing `on` timestamp value in `join`");
        } else {
          warn_once("expected `time` for `on` timestamp in `join`");
        }
      }
    });
  return values;
}

struct ExpiryItem {
  int64_t expire_ts = 0;
  uint32_t entry_id = 0;
  uint64_t generation = 0;

  auto operator>(ExpiryItem const& other) const noexcept -> bool {
    return expire_ts > other.expire_ts;
  }
};

/// Min-heap over `ExpiryItem` backed by a `std::vector` so retained capacity
/// can be accounted for in `tracked_bytes()` and released when drained.
class ExpiryMinHeap {
public:
  auto empty() const noexcept -> bool {
    return items_.empty();
  }

  auto size() const noexcept -> size_t {
    return items_.size();
  }

  auto capacity() const noexcept -> size_t {
    return items_.capacity();
  }

  auto capacity_bytes() const noexcept -> uint64_t {
    return static_cast<uint64_t>(items_.capacity()) * sizeof(ExpiryItem);
  }

  auto top() const noexcept -> ExpiryItem const& {
    return items_.front();
  }

  auto push(ExpiryItem item) -> void {
    items_.push_back(item);
    std::push_heap(items_.begin(), items_.end(), std::greater<>{});
  }

  auto pop() -> void {
    std::pop_heap(items_.begin(), items_.end(), std::greater<>{});
    items_.pop_back();
  }

  auto clear() -> void {
    std::vector<ExpiryItem>{}.swap(items_);
  }

  template <class Pred>
  auto compact(Pred&& keep) -> void {
    auto next = std::vector<ExpiryItem>{};
    for (auto const& item : items_) {
      if (keep(item)) {
        next.push_back(item);
      }
    }
    std::make_heap(next.begin(), next.end(), std::greater<>{});
    next.shrink_to_fit();
    items_.swap(next);
  }

private:
  std::vector<ExpiryItem> items_;
};

struct ArrivalItem {
  int64_t arrived_ns = 0;
  uint32_t entry_id = 0;
  uint64_t generation = 0;
};

struct KeyBucket {
  JoinKey key;
  uint64_t key_bytes = 0;
  std::vector<uint32_t> left_ids;
  std::vector<uint32_t> right_ids;
  bool alive = false;
};

struct LeftEntry {
  uint64_t generation = 0;
  int64_t ts = 0;
  int64_t arrived_ns = 0;
  uint32_t bucket_id = 0;
  uint32_t batch = 0;
  nova::storage::Index row = 0;
  bool alive = false;
  bool matched = false;
  Option<Candidate> best;
};

struct RightEntry {
  uint64_t generation = 0;
  uint64_t seq = 0;
  int64_t ts = 0;
  int64_t arrived_ns = 0;
  uint32_t bucket_id = 0;
  uint32_t batch = 0;
  nova::storage::Index row = 0;
  uint32_t pin_count = 0;
  bool alive = false;
  bool in_index = false;
};

enum class EmitStatus : uint8_t {
  emitted,
  chunk_full,
  row_too_large,
};

auto erase_id(std::vector<uint32_t>& ids, uint32_t target) -> void {
  auto it = std::ranges::find(ids, target);
  if (it != ids.end()) {
    *it = ids.back();
    ids.pop_back();
  }
}

class OutputCollector {
public:
  OutputCollector(bool emit_pairs, Option<ast::field_path> const& into,
                  location op_loc, BatchPool<nova::Events>& left_pool)
    : emit_pairs_{emit_pairs},
      into_{into},
      op_loc_{op_loc},
      left_pool_{left_pool} {
  }

  ~OutputCollector() {
    release_pins();
  }

  auto discard() -> void {
    left_asm_ = nova::EventsAssembler{};
    right_builder_ = nova::ArrayBuilder<nova::Data>{};
    emitted_rows_ = 0;
    emitted_bytes_ = 0;
    ready_chunk_ = None{};
    release_pins();
  }

  auto has_ready() const noexcept -> bool {
    return ready_chunk_.has_value();
  }

  auto take_ready() -> Option<nova::Events> {
    return std::exchange(ready_chunk_, None{});
  }

  auto try_emit_left_only(nova::Events const& left,
                          nova::storage::Index left_row, diagnostic_handler& dh,
                          Option<uint32_t> left_pool_id = None{})
    -> EmitStatus {
    TENZIR_ASSERT(not emit_pairs_);
    return try_append_left(left, left_row, 0, dh, left_pool_id);
  }

  auto try_emit_pair(nova::Events const& left, nova::storage::Index left_row,
                     Option<nova::RowView<nova::Data>> right_view,
                     diagnostic_handler& dh,
                     Option<uint32_t> left_pool_id = None{}) -> EmitStatus {
    TENZIR_ASSERT(emit_pairs_);
    auto const extra_bytes
      = right_view ? estimate_view_bytes(*right_view) : uint64_t{0};
    auto const st
      = try_append_left(left, left_row, extra_bytes, dh, left_pool_id);
    if (st != EmitStatus::emitted) {
      return st;
    }
    if (right_view) {
      nova::append_row(right_builder_, *right_view);
    } else {
      right_builder_.null();
    }
    return EmitStatus::emitted;
  }

  auto flush(diagnostic_handler& dh) -> void {
    if (emitted_rows_ == 0) {
      release_pins();
      return;
    }
    TENZIR_ASSERT(not ready_chunk_);
    auto left_events = std::move(left_asm_).finish();
    left_asm_ = nova::EventsAssembler{};
    release_pins();
    if (emit_pairs_) {
      auto out_mask = left_events.mask;
      ready_chunk_
        = combine_records(std::move(left_events), right_builder_.finish(),
                          std::move(out_mask), into_, dh);
      right_builder_ = nova::ArrayBuilder<nova::Data>{};
    } else {
      ready_chunk_ = std::move(left_events);
    }
    emitted_rows_ = 0;
    emitted_bytes_ = 0;
  }

private:
  auto try_append_left(nova::Events const& left, nova::storage::Index left_row,
                       uint64_t extra_bytes, diagnostic_handler& dh,
                       Option<uint32_t> left_pool_id) -> EmitStatus {
    auto const row_bytes = detail::saturating_add(
      estimate_event_row_bytes(left, left_row), extra_bytes);
    if (row_bytes > max_single_row_bytes) {
      diagnostic::error("`join` matched row exceeds maximum output batch size")
        .primary(op_loc_)
        .emit(dh);
      return EmitStatus::row_too_large;
    }
    if (will_overflow(row_bytes)) {
      flush(dh);
      return EmitStatus::chunk_full;
    }
    if (left_pool_id) {
      left_pool_.retain_row(*left_pool_id);
      pinned_left_batches_.push_back(*left_pool_id);
    }
    left_asm_.append(left, left_row);
    emitted_bytes_ = detail::saturating_add(emitted_bytes_, row_bytes);
    ++emitted_rows_;
    return EmitStatus::emitted;
  }

  auto will_overflow(uint64_t row_bytes) const noexcept -> bool {
    return emitted_rows_ >= output_chunk_rows
           or (emitted_rows_ > 0
               and detail::saturating_add(emitted_bytes_, row_bytes)
                     > output_chunk_bytes);
  }

  auto release_pins() -> void {
    for (auto const id : pinned_left_batches_) {
      if (id < left_pool_.slots.size() and left_pool_.slots[id].live_rows > 0) {
        left_pool_.release_row(id);
      }
    }
    pinned_left_batches_.clear();
  }

  bool emit_pairs_ = true;
  Option<ast::field_path> const& into_;
  location op_loc_ = location::unknown;
  BatchPool<nova::Events>& left_pool_;
  std::vector<uint32_t> pinned_left_batches_;
  nova::EventsAssembler left_asm_;
  nova::ArrayBuilder<nova::Data> right_builder_;
  nova::storage::Index emitted_rows_ = 0;
  uint64_t emitted_bytes_ = 0;
  Option<nova::Events> ready_chunk_;
};

class StreamingJoin final : public Operator<nova::Events, nova::Events> {
public:
  StreamingJoin(JoinArgs args, ir::pipeline right_pipe)
    : args_{std::move(args)}, right_pipe_{std::move(right_pipe)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (ctx.checkpoint_settings()) {
      phase_ = Phase::done;
      diagnostic::error("`join` does not support checkpointing yet")
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    if (args_.kind) {
      auto parsed = parse_kind(args_.kind->inner);
      TENZIR_ASSERT(parsed);
      kind_ = *parsed;
    }
    if (args_.pick) {
      auto parsed = parse_pick(args_.pick->inner);
      TENZIR_ASSERT(parsed);
      pick_ = *parsed;
    }
    store_payloads_ = kind_ == JoinKind::inner or kind_ == JoinKind::left;
    max_memory_
      = args_.max_memory ? args_.max_memory->inner : default_max_memory;
    memory_loc_
      = args_.max_memory ? args_.max_memory->source : args_.operator_location;
    if (args_.within) {
      before_ns_ = args_.within->inner.count();
      after_ns_ = args_.within->inner.count();
    } else {
      before_ns_ = args_.before ? args_.before->inner.count() : int64_t{0};
      after_ns_ = args_.after ? args_.after->inner.count() : int64_t{0};
    }
    tolerance_ns_
      = args_.tolerance ? args_.tolerance->inner.count() : int64_t{0};
    if (args_.idle_timeout) {
      idle_timeout_ns_ = args_.idle_timeout->inner.count();
    }
    auto states
      = co_await build_key_states(args_.keys, args_.operator_location, ctx);
    if (not states) {
      phase_ = Phase::done;
      co_return;
    }
    left_key_state_ = std::move(states->first);
    right_key_state_ = std::move(states->second);
    if (args_.on) {
      auto ts_pair
        = classify_timestamp_expr(std::move(*args_.on), ctx.reg(), ctx.dh());
      args_.on = None{};
      if (not ts_pair) {
        phase_ = Phase::done;
        co_return;
      }
      left_ts_loc_ = ts_pair->left_loc;
      right_ts_loc_ = ts_pair->right_loc;
      auto le = co_await nova::Evaluator::make(std::move(ts_pair->left), ctx);
      if (not le) {
        phase_ = Phase::done;
        co_return;
      }
      auto re = co_await nova::Evaluator::make(std::move(ts_pair->right), ctx);
      if (not re) {
        phase_ = Phase::done;
        co_return;
      }
      left_ts_eval_ = std::move(*le);
      right_ts_eval_ = std::move(*re);
      event_time_ = true;
    }
    auto const now_ns = steady_now_ns();
    last_left_activity_ns_ = now_ns;
    last_right_activity_ns_ = now_ns;
    auto [tx, rx] = channel<nova::Events>(right_channel_capacity);
    right_tx_ = std::move(tx);
    right_rx_ = std::move(rx);
    right_rx_active_.store(true, std::memory_order_release);
    phase_ = Phase::streaming;
    auto sub = co_await ctx.plan_and_spawn_sub<void>(data{"right"},
                                                     std::move(right_pipe_));
    if (not sub) {
      right_tx_ = None{};
      right_rx_active_.store(false, std::memory_order_release);
      phase_ = Phase::done;
      co_return;
    }
    right_sub_spawned_ = true;
    co_return;
  }

  auto state() -> OperatorState override {
    if (phase_ == Phase::done) {
      return OperatorState::done;
    }
    if (draining_output_.load(std::memory_order_acquire)) {
      return OperatorState::blocked;
    }
    return OperatorState::normal;
  }

  auto process_sub(SubKeyView key, nova::Events events,
                   Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push, ctx);
    if (right_discarding_.load(std::memory_order_acquire)
        or not events.mask.any()) {
      co_return;
    }
    TENZIR_ASSERT(right_tx_);
    try {
      auto token = folly::cancellation_token_merge(
        co_await folly::coro::co_current_cancellation_token,
        right_send_cancel_.getToken());
      co_await folly::coro::co_withCancellation(
        std::move(token), right_tx_->send(std::move(events)));
    } catch (folly::OperationCancelled const&) {
      // Cancelled during shutdown or failure; drop the remaining batch.
    }
    co_return;
  }

  auto finish_sub(SubKeyView key, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push, ctx);
    right_tx_ = None{};
    right_sub_spawned_ = false;
    co_return;
  }

  auto finish_sub(SubKeyView key, failure error, Push<nova::Events>& push,
                  OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key, error, push, ctx);
    right_discarding_.store(true, std::memory_order_release);
    right_tx_ = None{};
    right_sub_spawned_ = false;
    right_rx_active_.store(false, std::memory_order_release);
    next_deadline_ns_.store(no_deadline, std::memory_order_release);
    draining_output_.store(false, std::memory_order_release);
    clear_all_state();
    phase_ = Phase::done;
    wake_notify_.notify_one();
    co_return;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(ctx);
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (draining_output_.load(std::memory_order_acquire)) {
      co_await wake_notify_.wait();
      co_return WakeTick{};
    }
    auto const rx_active = right_rx_active_.load(std::memory_order_acquire);
    auto const deadline_ns = next_deadline_ns_.load(std::memory_order_acquire);
    if (rx_active and right_rx_) {
      if (deadline_ns != no_deadline) {
        auto const deadline = std::chrono::steady_clock::time_point{
          std::chrono::nanoseconds{deadline_ns}};
        auto [msg, wake, timer] = co_await folly::coro::collectAnyNoDiscard(
          right_rx_->recv(), wake_notify_.wait(), sleep_until(deadline));
        if (msg.hasValue()) {
          co_return RightMsg{std::move(msg).value()};
        }
        if (wake.hasValue()) {
          co_return WakeTick{};
        }
        timer.value();
        co_return TimerTick{};
      }
      auto [msg, wake] = co_await folly::coro::collectAnyNoDiscard(
        right_rx_->recv(), wake_notify_.wait());
      if (msg.hasValue()) {
        co_return RightMsg{std::move(msg).value()};
      }
      wake.value();
      co_return WakeTick{};
    }
    if (deadline_ns != no_deadline) {
      auto const deadline = std::chrono::steady_clock::time_point{
        std::chrono::nanoseconds{deadline_ns}};
      auto [wake, timer] = co_await folly::coro::collectAnyNoDiscard(
        wake_notify_.wait(), sleep_until(deadline));
      if (wake.hasValue()) {
        co_return WakeTick{};
      }
      timer.value();
      co_return TimerTick{};
    }
    co_await wake_notify_.wait();
    co_return WakeTick{};
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (phase_ == Phase::done) {
      co_return;
    }
    if (result.try_as<WakeTick>()) {
      co_await advance_pending_work(push, ctx);
      co_return;
    }
    if (auto* msg = result.try_as<RightMsg>()) {
      if (msg->events) {
        co_await handle_right_batch(std::move(*msg->events), push, ctx);
      } else {
        right_eof_ = true;
        right_rx_active_.store(false, std::memory_order_release);
        pending_expiry_drain_ = true;
        co_await advance_pending_work(push, ctx);
      }
      co_return;
    }
    pending_expiry_drain_ = true;
    co_await advance_pending_work(push, ctx);
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (phase_ == Phase::done or not input.mask.any()) {
      co_return;
    }
    TENZIR_ASSERT(not pending_left_batch_);
    TENZIR_ASSERT(not pending_right_batch_);
    auto const now_ns = steady_now_ns();
    last_left_activity_ns_ = now_ns;
    auto const input_len = input.length();
    auto base = prepare_batch(left_key_state_, left_ts_eval_, left_ts_loc_,
                              warned_invalid_left_ts_, std::move(input), now_ns,
                              ctx.dh());
    pending_left_batch_.emplace(PendingLeftBatch{
      std::move(base),
      /*cur_matched=*/false,
      /*cur_best=*/None{},
      /*cur_bucket_id=*/None{},
      /*to_store=*/{},
      nova::storage::BitMap::Mutable{nova::storage::BitMap{input_len, false}},
    });
    co_await advance_pending_work(push, ctx);
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (phase_ == Phase::done) {
      co_return FinalizeBehavior::done;
    }
    left_eof_ = true;
    // Defer unpinned right eviction if a left batch is still mid-probe (its
    // right_ids must remain stable until `pending_left_batch_` finishes).
    if (not pending_left_batch_) {
      evict_all_unpinned_right();
    }
    if (has_pending_work()) {
      co_await advance_pending_work(push, ctx);
    } else {
      co_await maybe_finish_if_settled(ctx);
    }
    if (phase_ == Phase::done) {
      co_return FinalizeBehavior::done;
    }
    update_next_deadline();
    co_return FinalizeBehavior::continue_;
  }

private:
  enum class Phase : uint8_t {
    streaming,
    done,
  };

  struct WakeTick {};
  struct TimerTick {};
  struct RightMsg {
    Option<nova::Events> events;
  };

  struct PreparedBatch {
    nova::Events events;
    int64_t now_ns = 0;
    KeyEvalResult eval_result;
    std::vector<Option<int64_t>> ts_values;
    std::vector<size_t> hash_for_row;
    std::vector<nova::storage::Index> active_rows;
    size_t row_cursor = 0;
    size_t probe_cursor = 0;
    bool cur_row_initialized = false;
    Option<int64_t> batch_max_ts;
    bool pre_expiry_done = false;
  };

  struct PendingLeftStore {
    nova::storage::Index orig_row = 0;
    int64_t ts = 0;
    uint32_t bucket_id = 0;
    bool matched = false;
    Option<Candidate> best;
  };

  struct PendingLeftBatch : PreparedBatch {
    bool cur_matched = false;
    Option<Candidate> cur_best;
    Option<uint32_t> cur_bucket_id;
    std::vector<PendingLeftStore> to_store;
    nova::storage::BitMap::Mutable retain_mask;
  };

  struct PendingRightStore {
    nova::storage::Index orig_row = 0;
    uint64_t seq = 0;
    int64_t ts = 0;
    uint32_t bucket_id = 0;
    uint32_t staged_pin_count = 0;
    bool need_index = false;
  };

  struct PendingRightBatch : PreparedBatch {
    uint64_t cur_seq = 0;
    uint32_t cur_staged_pins = 0;
    std::vector<uint32_t> cur_left_ids;
    std::vector<PendingRightStore> to_store;
    // Maps each improved open left entry ID to its single winning staged
    // candidate (`Candidate::right_id` stores the index in `to_store`), keeping
    // staging memory O(live_left_count_) instead of O(right_rows * left_rows).
    tsl::robin_map<uint32_t, Candidate> staged_best_for_left;
  };

  auto
  prepare_batch(SideKeyState& key_state, Option<nova::Evaluator>& ts_eval,
                location ts_loc, bool& warned_invalid_ts, nova::Events events,
                int64_t now_ns, diagnostic_handler& dh) -> PreparedBatch {
    auto eval_result = evaluate_keys(key_state, events, dh);
    auto ts_values = std::vector<Option<int64_t>>{};
    if (event_time_) {
      TENZIR_ASSERT(ts_eval);
      ts_values
        = extract_timestamps(*ts_eval, events, ts_loc, warned_invalid_ts, dh);
    }
    auto hash_for_row
      = std::vector<size_t>(detail::narrow<size_t>(events.length()),
                            std::numeric_limits<size_t>::max());
    auto hi = size_t{0};
    for (auto row : nova::storage::true_bits(eval_result.eligible_mask)) {
      hash_for_row[detail::narrow<size_t>(row)] = hi++;
    }
    auto active_rows = std::vector<nova::storage::Index>{};
    active_rows.reserve(detail::narrow<size_t>(events.active_count()));
    for (auto row : nova::storage::true_bits(events.mask)) {
      active_rows.push_back(row);
    }
    return PreparedBatch{
      .events = std::move(events),
      .now_ns = now_ns,
      .eval_result = std::move(eval_result),
      .ts_values = std::move(ts_values),
      .hash_for_row = std::move(hash_for_row),
      .active_rows = std::move(active_rows),
      .row_cursor = 0,
      .probe_cursor = 0,
      .cur_row_initialized = false,
      .batch_max_ts = None{},
      .pre_expiry_done = false,
    };
  }

  auto in_window(int64_t left_ts, int64_t right_ts) const noexcept -> bool {
    auto const lo = detail::saturating_sub(left_ts, before_ns_);
    auto const hi = detail::saturating_add(left_ts, after_ns_);
    return right_ts >= lo and right_ts <= hi;
  }

  auto make_collector() -> OutputCollector {
    return OutputCollector{store_payloads_, args_.into, args_.operator_location,
                           left_pool_};
  }

  auto check_emit(EmitStatus st) noexcept -> bool {
    if (st == EmitStatus::row_too_large) {
      failed_ = true;
      return false;
    }
    return st == EmitStatus::emitted;
  }

  auto check_memory_limit(diagnostic_handler& dh, uint64_t extra_bytes = 0)
    -> bool {
    if (detail::saturating_add(tracked_bytes(), extra_bytes) > max_memory_) {
      fail_memory_limit(dh);
      return false;
    }
    return true;
  }

  auto emit_final_left(nova::Events const& left, nova::storage::Index row,
                       bool matched, Option<Candidate> const& best,
                       OutputCollector& collector, diagnostic_handler& dh,
                       Option<uint32_t> left_pool_id = None{}) -> EmitStatus {
    if (pick_ == PickMode::all) {
      if (not matched) {
        if (kind_ == JoinKind::left) {
          return collector.try_emit_pair(left, row, None{}, dh, left_pool_id);
        }
        if (kind_ == JoinKind::anti) {
          return collector.try_emit_left_only(left, row, dh, left_pool_id);
        }
      }
      return EmitStatus::emitted;
    }
    if (best) {
      auto const rview = right_row_view(right_entries_[best->right_id]);
      return collector.try_emit_pair(left, row, rview, dh, left_pool_id);
    }
    if (kind_ == JoinKind::left) {
      return collector.try_emit_pair(left, row, None{}, dh, left_pool_id);
    }
    return EmitStatus::emitted;
  }

  auto right_row_view(RightEntry const& r) const -> nova::RowView<nova::Data> {
    return nova::RowView<nova::Data>{right_pool_.get(r.batch).get(r.row)};
  }

  auto has_pending_work() const noexcept -> bool {
    return pending_left_batch_.has_value() or pending_right_batch_.has_value()
           or pending_expiry_drain_ or (right_eof_ and live_left_count_ > 0);
  }

  /// Runs at most ONE bounded chunk's worth of pending batch / expiry / EOF
  /// work, pushes that chunk downstream, and keeps `draining_output_` armed
  /// (so `OperatorState::blocked` prevents new input while paused) if further
  /// work remains.
  auto advance_pending_work(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> {
    auto collector = make_collector();
    auto paused = false;
    if (pending_left_batch_) {
      paused = not step_left_batch(*pending_left_batch_, collector, ctx.dh());
      if (not paused and not failed_) {
        pending_left_batch_ = None{};
        if (left_eof_) {
          evict_all_unpinned_right();
        }
      }
    } else if (pending_right_batch_) {
      paused = not step_right_batch(*pending_right_batch_, collector, ctx.dh());
      if (not paused and not failed_) {
        pending_right_batch_ = None{};
      }
    }
    if (not failed_ and not paused and not collector.has_ready()
        and not pending_left_batch_ and not pending_right_batch_) {
      paused = not step_pending_finalizations(collector, ctx.dh());
    }
    if (not failed_ and not paused) {
      collector.flush(ctx.dh());
    }
    if (not failed_) {
      check_memory_limit(ctx.dh());
    }
    if (failed_) {
      collector.discard();
      co_await abort_on_failure(ctx);
      co_return;
    }
    auto ready_chunk = collector.take_ready();
    maybe_compact_heaps();
    update_next_deadline();
    if (ready_chunk) {
      co_await push(std::move(*ready_chunk));
    }
    if (has_pending_work()) {
      draining_output_.store(true, std::memory_order_release);
      wake_notify_.notify_one();
    } else {
      draining_output_.store(false, std::memory_order_release);
    }
    co_await maybe_finish_if_settled(ctx);
  }

  /// Processes rows of `p` until either `p` completes (`return true`) or
  /// `collector` seals a full chunk (`return false`) or an error occurs.
  auto step_left_batch(PendingLeftBatch& p, OutputCollector& collector,
                       diagnostic_handler& dh) -> bool {
    if (not event_time_ and not p.pre_expiry_done) {
      if (not advance_processing_time(p.now_ns, collector, dh)) {
        return false;
      }
      p.pre_expiry_done = true;
    }
    auto key_view = JoinKeyView{};
    key_view.reserve(p.eval_result.columns.size());
    while (p.row_cursor < p.active_rows.size()) {
      auto const row = p.active_rows[p.row_cursor];
      auto const ts = event_time_ ? p.ts_values[detail::narrow<size_t>(row)]
                                  : Option<int64_t>{p.now_ns};
      auto const hi = p.hash_for_row[detail::narrow<size_t>(row)];
      auto const key_eligible = hi != std::numeric_limits<size_t>::max();
      if (not p.cur_row_initialized) {
        p.cur_row_initialized = true;
        p.probe_cursor = 0;
        p.cur_matched = false;
        p.cur_best = None{};
        p.cur_bucket_id = None{};
        if (ts and (not p.batch_max_ts or *ts > *p.batch_max_ts)) {
          p.batch_max_ts = *ts;
        }
        if (ts and key_eligible and event_time_ and left_watermark_
            and *ts < *left_watermark_ and not warned_late_left_) {
          warned_late_left_ = true;
          diagnostic::warning(
            "late input event arrived behind watermark; some matching "
            "right-hand events may already have been evicted")
            .primary(left_ts_loc_)
            .hint("increase `tolerance` if events arrive out of order")
            .emit(dh);
        }
        if (ts and key_eligible) {
          key_view.clear();
          for (auto const& col : p.eval_result.columns) {
            key_view.push_back(col.get(row));
          }
          p.cur_bucket_id = find_bucket(key_view, p.eval_result.hashes[hi]);
        }
      }
      if (not ts or not key_eligible) {
        if (not check_emit(emit_final_left(p.events, row, /*matched=*/false,
                                           None{}, collector, dh))) {
          return false;
        }
        ++p.row_cursor;
        p.cur_row_initialized = false;
        continue;
      }
      auto const row_ts = *ts;
      if (p.cur_bucket_id) {
        auto const& right_ids = buckets_[*p.cur_bucket_id].right_ids;
        while (p.probe_cursor < right_ids.size()) {
          auto const rid = right_ids[p.probe_cursor];
          auto const& r = right_entries_[rid];
          if (not r.alive or not r.in_index or not in_window(row_ts, r.ts)) {
            ++p.probe_cursor;
            continue;
          }
          had_match_ = true;
          if (pick_ == PickMode::all) {
            if (kind_ == JoinKind::inner or kind_ == JoinKind::left) {
              if (not check_emit(collector.try_emit_pair(
                    p.events, row, right_row_view(r), dh))) {
                return false;
              }
              p.cur_matched = true;
              ++p.probe_cursor;
            } else if (kind_ == JoinKind::semi) {
              if (not p.cur_matched) {
                if (not check_emit(
                      collector.try_emit_left_only(p.events, row, dh))) {
                  return false;
                }
                p.cur_matched = true;
              }
              p.probe_cursor = right_ids.size();
              break;
            } else {
              p.cur_matched = true;
              p.probe_cursor = right_ids.size();
              break;
            }
          } else {
            auto const cand = Candidate{r.ts, r.seq, rid};
            if (not p.cur_best
                or is_better_candidate(pick_, row_ts, cand, *p.cur_best)) {
              p.cur_best = cand;
            }
            ++p.probe_cursor;
          }
        }
      }
      auto const expire_ts = detail::saturating_add(row_ts, after_ns_);
      auto const already_final
        = right_eof_ or (right_watermark_ and expire_ts < *right_watermark_);
      if (already_final) {
        if (not check_emit(emit_final_left(p.events, row, p.cur_matched,
                                           p.cur_best, collector, dh))) {
          return false;
        }
        ++p.row_cursor;
        p.cur_row_initialized = false;
        continue;
      }
      if (not(pick_ == PickMode::all and p.cur_matched
              and (kind_ == JoinKind::semi or kind_ == JoinKind::anti))) {
        auto bucket_id = p.cur_bucket_id;
        if (not bucket_id) {
          key_view.clear();
          for (auto const& col : p.eval_result.columns) {
            key_view.push_back(col.get(row));
          }
          bucket_id
            = get_or_create_bucket(key_view, p.eval_result.hashes[hi], dh);
          if (not bucket_id) {
            failed_ = true;
            return false;
          }
        }
        if (p.cur_best) {
          ++right_entries_[p.cur_best->right_id].pin_count;
        }
        p.retain_mask.set(row, true);
        p.to_store.push_back(PendingLeftStore{
          .orig_row = row,
          .ts = row_ts,
          .bucket_id = *bucket_id,
          .matched = p.cur_matched,
          .best = p.cur_best,
        });
        if (not check_memory_limit(dh)) {
          return false;
        }
      }
      ++p.row_cursor;
      p.cur_row_initialized = false;
    }
    // Flush any rows referencing `p.events` before `p.events` is compacted or
    // moved into `left_pool_`.
    collector.flush(dh);
    if (not p.to_store.empty()) {
      auto final_retain_mask = std::move(p.retain_mask).finish();
      auto compacted = false;
      if (static_cast<int64_t>(p.to_store.size()) * 2
          < static_cast<int64_t>(p.events.length())) {
        p.events.mask = final_retain_mask;
        p.events = nova::physically_reduce_to_mask(std::move(p.events));
        compacted = true;
      }
      auto const batch_bytes = estimate_events_bytes(p.events);
      if (not check_memory_limit(dh, batch_bytes)) {
        return false;
      }
      auto const batch_id
        = left_pool_.insert(std::move(p.events),
                            detail::narrow<uint32_t>(p.to_store.size()),
                            batch_bytes);
      auto dense_row = nova::storage::Index{0};
      for (auto const& ps : p.to_store) {
        auto const stored_row = compacted ? dense_row++ : ps.orig_row;
        auto const left_id = alloc_left_entry(LeftEntry{
          .generation = 0,
          .ts = ps.ts,
          .arrived_ns = p.now_ns,
          .bucket_id = ps.bucket_id,
          .batch = batch_id,
          .row = stored_row,
          .alive = true,
          .matched = ps.matched,
          .best = ps.best,
        });
        auto const gen = left_entries_[left_id].generation;
        append_bucket_id(buckets_[ps.bucket_id].left_ids, left_id);
        auto const expire_ts = detail::saturating_add(ps.ts, after_ns_);
        left_expiry_heap_.push(ExpiryItem{expire_ts, left_id, gen});
        if (idle_timeout_ns_) {
          left_arrival_queue_.push_back(ArrivalItem{p.now_ns, left_id, gen});
        }
      }
      std::vector<PendingLeftStore>{}.swap(p.to_store);
      if (not check_memory_limit(dh)) {
        return false;
      }
    }
    if (event_time_ and p.batch_max_ts) {
      if (not left_max_ts_ or *p.batch_max_ts > *left_max_ts_) {
        left_max_ts_ = *p.batch_max_ts;
        auto const next_wm
          = detail::saturating_sub(*left_max_ts_, tolerance_ns_);
        if (not left_watermark_ or next_wm > *left_watermark_) {
          left_watermark_ = next_wm;
          evict_expired_right(*left_watermark_);
        }
      }
    }
    if (idle_timeout_ns_) {
      pending_expiry_drain_ = true;
    }
    return true;
  }

  auto handle_right_batch(nova::Events events, Push<nova::Events>& push,
                          OpCtx& ctx) -> Task<void> {
    if (not events.mask.any()) {
      co_return;
    }
    TENZIR_ASSERT(not pending_left_batch_);
    TENZIR_ASSERT(not pending_right_batch_);
    auto const now_ns = steady_now_ns();
    last_right_activity_ns_ = now_ns;
    auto base = prepare_batch(right_key_state_, right_ts_eval_, right_ts_loc_,
                              warned_invalid_right_ts_, std::move(events),
                              now_ns, ctx.dh());
    pending_right_batch_.emplace(PendingRightBatch{
      std::move(base),
      /*cur_seq=*/0,
      /*cur_staged_pins=*/0,
      /*cur_left_ids=*/{},
      /*to_store=*/{},
      /*staged_best_for_left=*/{},
    });
    co_await advance_pending_work(push, ctx);
  }

  auto step_right_batch(PendingRightBatch& p, OutputCollector& collector,
                        diagnostic_handler& dh) -> bool {
    if (not event_time_ and not p.pre_expiry_done) {
      if (not advance_processing_time(p.now_ns, collector, dh)) {
        return false;
      }
      p.pre_expiry_done = true;
    }
    auto key_view = JoinKeyView{};
    key_view.reserve(p.eval_result.columns.size());
    while (p.row_cursor < p.active_rows.size()) {
      auto const row = p.active_rows[p.row_cursor];
      auto const ts = event_time_ ? p.ts_values[detail::narrow<size_t>(row)]
                                  : Option<int64_t>{p.now_ns};
      if (not ts) {
        ++p.row_cursor;
        p.cur_row_initialized = false;
        continue;
      }
      auto const row_ts = *ts;
      auto const hi = p.hash_for_row[detail::narrow<size_t>(row)];
      if (not p.cur_row_initialized) {
        p.cur_row_initialized = true;
        p.probe_cursor = 0;
        p.cur_staged_pins = 0;
        p.cur_left_ids.clear();
        if (not p.batch_max_ts or row_ts > *p.batch_max_ts) {
          p.batch_max_ts = row_ts;
        }
        if (hi == std::numeric_limits<size_t>::max()) {
          ++p.row_cursor;
          p.cur_row_initialized = false;
          continue;
        }
        p.cur_seq = ++next_right_seq_;
        if (event_time_ and right_watermark_ and row_ts < *right_watermark_
            and not warned_late_right_) {
          warned_late_right_ = true;
          diagnostic::warning(
            "late right-hand event arrived behind watermark; some matching "
            "input events may already have been finalized")
            .primary(right_ts_loc_)
            .hint("increase `tolerance` if events arrive out of order")
            .emit(dh);
        }
        key_view.clear();
        for (auto const& col : p.eval_result.columns) {
          key_view.push_back(col.get(row));
        }
        if (auto bucket_id = find_bucket(key_view, p.eval_result.hashes[hi])) {
          p.cur_left_ids = buckets_[*bucket_id].left_ids;
        }
      }
      if (hi == std::numeric_limits<size_t>::max()) {
        ++p.row_cursor;
        p.cur_row_initialized = false;
        continue;
      }
      auto const rview_direct
        = store_payloads_
            ? Option<nova::RowView<nova::Data>>{nova::RowView<nova::Data>{
                p.events.data.get(row)}}
            : None{};
      auto const next_store_idx = detail::narrow<uint32_t>(p.to_store.size());
      while (p.probe_cursor < p.cur_left_ids.size()) {
        auto const lid = p.cur_left_ids[p.probe_cursor];
        auto& l = left_entries_[lid];
        if (not l.alive or not in_window(l.ts, row_ts)) {
          ++p.probe_cursor;
          continue;
        }
        had_match_ = true;
        if (pick_ == PickMode::all) {
          if (kind_ == JoinKind::inner or kind_ == JoinKind::left) {
            if (not check_emit(collector.try_emit_pair(
                  left_pool_.get(l.batch), l.row, rview_direct, dh, l.batch))) {
              return false;
            }
            l.matched = true;
          } else if (kind_ == JoinKind::semi) {
            if (not l.matched) {
              if (not check_emit(collector.try_emit_left_only(
                    left_pool_.get(l.batch), l.row, dh, l.batch))) {
                return false;
              }
              l.matched = true;
            }
            retire_left_entry(lid);
          } else {
            l.matched = true;
            retire_left_entry(lid);
          }
        } else {
          auto const cand = Candidate{row_ts, p.cur_seq, next_store_idx};
          auto it = p.staged_best_for_left.find(lid);
          auto const* current_best = it != p.staged_best_for_left.end()
                                       ? &it->second
                                       : (l.best ? &*l.best : nullptr);
          if (not current_best
              or is_better_candidate(pick_, l.ts, cand, *current_best)) {
            if (it != p.staged_best_for_left.end()) {
              TENZIR_ASSERT_GT(p.to_store[it->second.right_id].staged_pin_count,
                               0u);
              --p.to_store[it->second.right_id].staged_pin_count;
              it.value() = cand;
            } else {
              p.staged_best_for_left.emplace(lid, cand);
              if (not check_memory_limit(dh)) {
                return false;
              }
            }
            ++p.cur_staged_pins;
          }
        }
        ++p.probe_cursor;
      }
      auto const expire_ts = detail::saturating_add(row_ts, before_ns_);
      auto const need_index
        = not left_eof_
          and not(left_watermark_ and expire_ts < *left_watermark_);
      auto const need_entry = need_index or p.cur_staged_pins > 0;
      if (need_entry) {
        auto bucket_id = uint32_t{0};
        if (need_index) {
          key_view.clear();
          for (auto const& col : p.eval_result.columns) {
            key_view.push_back(col.get(row));
          }
          auto created
            = get_or_create_bucket(key_view, p.eval_result.hashes[hi], dh);
          if (not created) {
            failed_ = true;
            return false;
          }
          bucket_id = *created;
        }
        p.to_store.push_back(PendingRightStore{
          .orig_row = row,
          .seq = p.cur_seq,
          .ts = row_ts,
          .bucket_id = bucket_id,
          .staged_pin_count = p.cur_staged_pins,
          .need_index = need_index,
        });
        if (not check_memory_limit(dh)) {
          return false;
        }
      }
      ++p.row_cursor;
      p.cur_row_initialized = false;
    }
    if (not p.to_store.empty()) {
      auto kept_count = size_t{0};
      auto retain_mask = nova::storage::BitMap::Mutable{
        nova::storage::BitMap{p.events.length(), false}};
      for (auto const& pr : p.to_store) {
        if (pr.need_index or pr.staged_pin_count > 0) {
          ++kept_count;
          if (store_payloads_) {
            retain_mask.set(pr.orig_row, true);
          }
        }
      }
      if (kept_count > 0) {
        auto batch_id = uint32_t{0};
        auto compacted = false;
        if (store_payloads_) {
          auto final_retain_mask = std::move(retain_mask).finish();
          if (static_cast<int64_t>(kept_count) * 2
              < static_cast<int64_t>(p.events.length())) {
            p.events.mask = final_retain_mask;
            p.events = nova::physically_reduce_to_mask(std::move(p.events));
            compacted = true;
          }
          auto batch_data = std::move(p.events.data);
          auto const batch_bytes
            = static_cast<uint64_t>(batch_data.approx_bytes());
          if (not check_memory_limit(dh, batch_bytes)) {
            return false;
          }
          batch_id = right_pool_.insert(std::move(batch_data),
                                        detail::narrow<uint32_t>(kept_count),
                                        batch_bytes);
        }
        auto store_idx_to_right_id = std::vector<uint32_t>(
          p.to_store.size(), std::numeric_limits<uint32_t>::max());
        auto dense_row = nova::storage::Index{0};
        for (auto i = size_t{0}; i < p.to_store.size(); ++i) {
          auto const& pr = p.to_store[i];
          if (not pr.need_index and pr.staged_pin_count == 0) {
            continue;
          }
          auto const stored_row = compacted ? dense_row++ : pr.orig_row;
          auto const right_id = alloc_right_entry(RightEntry{
            .generation = 0,
            .seq = pr.seq,
            .ts = pr.ts,
            .arrived_ns = p.now_ns,
            .bucket_id = pr.bucket_id,
            .batch = batch_id,
            .row = stored_row,
            .pin_count = 0,
            .alive = true,
            .in_index = pr.need_index,
          });
          store_idx_to_right_id[i] = right_id;
          if (pr.need_index) {
            auto const gen = right_entries_[right_id].generation;
            append_bucket_id(buckets_[pr.bucket_id].right_ids, right_id);
            auto const expire_ts = detail::saturating_add(pr.ts, before_ns_);
            right_expiry_heap_.push(ExpiryItem{expire_ts, right_id, gen});
            if (idle_timeout_ns_) {
              right_arrival_queue_.push_back(
                ArrivalItem{p.now_ns, right_id, gen});
            }
          }
        }
        for (auto const& [lid, staged] : p.staged_best_for_left) {
          auto const right_id = store_idx_to_right_id[staged.right_id];
          TENZIR_ASSERT_NEQ(right_id, std::numeric_limits<uint32_t>::max());
          auto& l = left_entries_[lid];
          if (not l.alive) {
            continue;
          }
          if (l.best) {
            unpin_right(l.best->right_id);
          }
          l.best = Candidate{staged.right_ts, staged.right_seq, right_id};
          ++right_entries_[right_id].pin_count;
        }
        tsl::robin_map<uint32_t, Candidate>{}.swap(p.staged_best_for_left);
        std::vector<PendingRightStore>{}.swap(p.to_store);
        if (not check_memory_limit(dh)) {
          return false;
        }
      } else {
        tsl::robin_map<uint32_t, Candidate>{}.swap(p.staged_best_for_left);
        std::vector<PendingRightStore>{}.swap(p.to_store);
      }
    }
    if (event_time_ and p.batch_max_ts) {
      if (not right_max_ts_ or *p.batch_max_ts > *right_max_ts_) {
        right_max_ts_ = *p.batch_max_ts;
        auto const next_wm
          = detail::saturating_sub(*right_max_ts_, tolerance_ns_);
        if (not right_watermark_ or next_wm > *right_watermark_) {
          right_watermark_ = next_wm;
          pending_expiry_drain_ = true;
        }
      }
    }
    if (idle_timeout_ns_) {
      pending_expiry_drain_ = true;
    }
    return true;
  }

  auto step_pending_finalizations(OutputCollector& collector,
                                  diagnostic_handler& dh) -> bool {
    if (right_eof_) {
      if (not finalize_left_until(None{}, collector, dh)) {
        return false;
      }
      pending_expiry_drain_ = false;
      return true;
    }
    if (not pending_expiry_drain_) {
      return true;
    }
    auto const now_ns = steady_now_ns();
    if (not event_time_) {
      if (not advance_processing_time(now_ns, collector, dh)) {
        return false;
      }
    } else {
      if (right_watermark_
          and not finalize_left_until(*right_watermark_, collector, dh)) {
        return false;
      }
      if (idle_timeout_ns_
          and not process_idle_expiries(now_ns, collector, dh)) {
        return false;
      }
    }
    pending_expiry_drain_ = false;
    return true;
  }

  auto advance_processing_time(int64_t now_ns, OutputCollector& collector,
                               diagnostic_handler& dh) -> bool {
    if (not left_watermark_ or now_ns > *left_watermark_) {
      left_watermark_ = now_ns;
      evict_expired_right(now_ns);
    }
    if (not right_watermark_ or now_ns > *right_watermark_) {
      right_watermark_ = now_ns;
    }
    return finalize_left_until(*right_watermark_, collector, dh);
  }

  auto is_live_left(uint32_t id, uint64_t gen) const noexcept -> bool {
    return left_entries_[id].alive and left_entries_[id].generation == gen;
  }

  auto is_indexed_right(uint32_t id, uint64_t gen) const noexcept -> bool {
    return right_entries_[id].alive and right_entries_[id].in_index
           and right_entries_[id].generation == gen;
  }

  auto process_idle_expiries(int64_t now_ns, OutputCollector& collector,
                             diagnostic_handler& dh) -> bool {
    TENZIR_ASSERT(idle_timeout_ns_);
    auto const timeout = *idle_timeout_ns_;
    if (now_ns >= detail::saturating_add(last_right_activity_ns_, timeout)) {
      while (not left_arrival_queue_.empty()) {
        auto const front = left_arrival_queue_.front();
        if (not is_live_left(front.entry_id, front.generation)) {
          left_arrival_queue_.pop_front();
          continue;
        }
        if (now_ns < detail::saturating_add(front.arrived_ns, timeout)) {
          break;
        }
        if (not check_emit(
              finalize_left_entry(front.entry_id, collector, dh))) {
          return false;
        }
        left_arrival_queue_.pop_front();
      }
    }
    if (now_ns >= detail::saturating_add(last_left_activity_ns_, timeout)) {
      while (not right_arrival_queue_.empty()) {
        auto const front = right_arrival_queue_.front();
        if (not is_indexed_right(front.entry_id, front.generation)) {
          right_arrival_queue_.pop_front();
          continue;
        }
        if (now_ns < detail::saturating_add(front.arrived_ns, timeout)) {
          break;
        }
        right_arrival_queue_.pop_front();
        evict_right_from_index(front.entry_id);
      }
    }
    return true;
  }

  auto finalize_left_until(Option<int64_t> watermark_ns,
                           OutputCollector& collector, diagnostic_handler& dh)
    -> bool {
    while (not left_expiry_heap_.empty()) {
      auto const top = left_expiry_heap_.top();
      if (not is_live_left(top.entry_id, top.generation)) {
        left_expiry_heap_.pop();
        continue;
      }
      if (watermark_ns and top.expire_ts >= *watermark_ns) {
        break;
      }
      if (not check_emit(finalize_left_entry(top.entry_id, collector, dh))) {
        return false;
      }
      left_expiry_heap_.pop();
    }
    if (not watermark_ns) {
      left_arrival_queue_.clear();
    }
    return true;
  }

  auto finalize_left_entry(uint32_t left_id, OutputCollector& collector,
                           diagnostic_handler& dh) -> EmitStatus {
    auto const& l = left_entries_[left_id];
    TENZIR_ASSERT(l.alive);
    auto const st = emit_final_left(left_pool_.get(l.batch), l.row, l.matched,
                                    l.best, collector, dh, l.batch);
    if (st == EmitStatus::emitted) {
      retire_left_entry(left_id);
    }
    return st;
  }

  auto retire_left_entry(uint32_t left_id) -> void {
    auto& l = left_entries_[left_id];
    if (not l.alive) {
      return;
    }
    if (l.best) {
      unpin_right(l.best->right_id);
      l.best = None{};
    }
    remove_bucket_id(l.bucket_id, buckets_[l.bucket_id].left_ids, left_id);
    left_pool_.release_row(l.batch);
    l.alive = false;
    --live_left_count_;
    free_left_ids_.push_back(left_id);
  }

  auto evict_expired_right(int64_t watermark_ns) -> void {
    while (not right_expiry_heap_.empty()) {
      auto const top = right_expiry_heap_.top();
      if (not is_indexed_right(top.entry_id, top.generation)) {
        right_expiry_heap_.pop();
        continue;
      }
      if (top.expire_ts >= watermark_ns) {
        break;
      }
      right_expiry_heap_.pop();
      evict_right_from_index(top.entry_id);
    }
  }

  auto evict_all_unpinned_right() -> void {
    while (not right_expiry_heap_.empty()) {
      auto const top = right_expiry_heap_.top();
      right_expiry_heap_.pop();
      if (is_indexed_right(top.entry_id, top.generation)) {
        evict_right_from_index(top.entry_id);
      }
    }
    right_arrival_queue_.clear();
  }

  auto evict_right_from_index(uint32_t right_id) -> void {
    auto& r = right_entries_[right_id];
    if (not r.alive or not r.in_index) {
      return;
    }
    remove_bucket_id(r.bucket_id, buckets_[r.bucket_id].right_ids, right_id);
    r.in_index = false;
    --indexed_right_count_;
    if (r.pin_count == 0) {
      destroy_right_entry(right_id);
    }
  }

  auto unpin_right(uint32_t right_id) -> void {
    auto& r = right_entries_[right_id];
    TENZIR_ASSERT(r.alive);
    TENZIR_ASSERT_GT(r.pin_count, 0u);
    if (--r.pin_count == 0 and not r.in_index) {
      destroy_right_entry(right_id);
    }
  }

  auto destroy_right_entry(uint32_t right_id) -> void {
    auto& r = right_entries_[right_id];
    if (not r.alive) {
      return;
    }
    if (store_payloads_) {
      right_pool_.release_row(r.batch);
    }
    r.alive = false;
    free_right_ids_.push_back(right_id);
  }

  auto find_bucket(JoinKeyView const& key_view, uint64_t hash) const
    -> Option<uint32_t> {
    auto it = key_to_bucket_.find(key_view, hash);
    if (it == key_to_bucket_.end()) {
      return None{};
    }
    return it->second;
  }

  auto get_or_create_bucket(JoinKeyView const& key_view, uint64_t hash,
                            diagnostic_handler& dh) -> Option<uint32_t> {
    if (auto existing = find_bucket(key_view, hash)) {
      return existing;
    }
    // Each bucket key is stored twice: once in the bucket (to erase the map
    // entry on reclaim) and once as the `key_to_bucket_` map key. Charge both
    // heap copies.
    auto const predicted
      = detail::saturating_add(estimate_key_view_heap_bytes(key_view),
                               estimate_key_view_heap_bytes(key_view));
    if (not check_memory_limit(dh, predicted)) {
      return None{};
    }
    auto materialized = materialize_key(key_view);
    auto const key_heap_bytes = estimate_key_heap_bytes(materialized);
    auto const key_bytes
      = detail::saturating_add(key_heap_bytes, key_heap_bytes);
    auto id = uint32_t{0};
    if (not free_bucket_ids_.empty()) {
      id = free_bucket_ids_.back();
      free_bucket_ids_.pop_back();
    } else {
      id = detail::narrow<uint32_t>(buckets_.size());
      buckets_.emplace_back();
    }
    auto& b = buckets_[id];
    b.key = std::move(materialized);
    b.key_bytes = key_bytes;
    b.left_ids.clear();
    b.right_ids.clear();
    b.alive = true;
    key_to_bucket_.emplace(b.key, id);
    keys_bytes_ = detail::saturating_add(keys_bytes_, key_bytes);
    return id;
  }

  auto append_bucket_id(std::vector<uint32_t>& ids, uint32_t id) -> void {
    auto const old_cap = ids.capacity();
    ids.push_back(id);
    if (ids.capacity() > old_cap) {
      bucket_lists_bytes_ = detail::saturating_add(
        bucket_lists_bytes_,
        static_cast<uint64_t>((ids.capacity() - old_cap) * sizeof(uint32_t)));
    }
  }

  auto remove_bucket_id(uint32_t bucket_id, std::vector<uint32_t>& ids,
                        uint32_t id) -> void {
    erase_id(ids, id);
    maybe_reclaim_bucket(bucket_id);
  }

  auto maybe_reclaim_bucket(uint32_t bucket_id) -> void {
    auto& b = buckets_[bucket_id];
    if (not b.alive or not b.left_ids.empty() or not b.right_ids.empty()) {
      return;
    }
    key_to_bucket_.erase(b.key);
    keys_bytes_ = keys_bytes_ >= b.key_bytes ? keys_bytes_ - b.key_bytes : 0;
    auto const list_cap_bytes
      = static_cast<uint64_t>(b.left_ids.capacity() + b.right_ids.capacity())
        * sizeof(uint32_t);
    bucket_lists_bytes_ = bucket_lists_bytes_ >= list_cap_bytes
                            ? bucket_lists_bytes_ - list_cap_bytes
                            : 0;
    JoinKey{}.swap(b.key);
    b.key_bytes = 0;
    std::vector<uint32_t>{}.swap(b.left_ids);
    std::vector<uint32_t>{}.swap(b.right_ids);
    b.alive = false;
    free_bucket_ids_.push_back(bucket_id);
  }

  auto alloc_left_entry(LeftEntry entry) -> uint32_t {
    entry.generation = ++next_left_gen_;
    ++live_left_count_;
    if (not free_left_ids_.empty()) {
      auto const id = free_left_ids_.back();
      free_left_ids_.pop_back();
      left_entries_[id] = std::move(entry);
      return id;
    }
    auto const id = detail::narrow<uint32_t>(left_entries_.size());
    left_entries_.push_back(std::move(entry));
    return id;
  }

  auto alloc_right_entry(RightEntry entry) -> uint32_t {
    entry.generation = ++next_right_gen_;
    if (entry.in_index) {
      ++indexed_right_count_;
    }
    if (not free_right_ids_.empty()) {
      auto const id = free_right_ids_.back();
      free_right_ids_.pop_back();
      right_entries_[id] = std::move(entry);
      return id;
    }
    auto const id = detail::narrow<uint32_t>(right_entries_.size());
    right_entries_.push_back(std::move(entry));
    return id;
  }

  auto maybe_compact_heaps() -> void {
    if (left_expiry_heap_.size() > live_left_count_ * 2 + 1024
        or left_expiry_heap_.capacity() > live_left_count_ * 4 + 1024) {
      left_expiry_heap_.compact([this](ExpiryItem const& item) {
        return is_live_left(item.entry_id, item.generation);
      });
    }
    if (right_expiry_heap_.size() > indexed_right_count_ * 2 + 1024
        or right_expiry_heap_.capacity() > indexed_right_count_ * 4 + 1024) {
      right_expiry_heap_.compact([this](ExpiryItem const& item) {
        return is_indexed_right(item.entry_id, item.generation);
      });
    }
  }

  auto update_next_deadline() -> void {
    auto next = no_deadline;
    if (not event_time_) {
      while (not left_expiry_heap_.empty()
             and not is_live_left(left_expiry_heap_.top().entry_id,
                                  left_expiry_heap_.top().generation)) {
        left_expiry_heap_.pop();
      }
      if (not left_expiry_heap_.empty()) {
        next = std::min(next, detail::saturating_add(
                                left_expiry_heap_.top().expire_ts, int64_t{1}));
      }
      while (not right_expiry_heap_.empty()
             and not is_indexed_right(right_expiry_heap_.top().entry_id,
                                      right_expiry_heap_.top().generation)) {
        right_expiry_heap_.pop();
      }
      if (not right_expiry_heap_.empty()) {
        next
          = std::min(next, detail::saturating_add(
                             right_expiry_heap_.top().expire_ts, int64_t{1}));
      }
    } else if (idle_timeout_ns_) {
      auto const timeout = *idle_timeout_ns_;
      while (not left_arrival_queue_.empty()
             and not is_live_left(left_arrival_queue_.front().entry_id,
                                  left_arrival_queue_.front().generation)) {
        left_arrival_queue_.pop_front();
      }
      if (not left_arrival_queue_.empty()) {
        auto const base = std::max(left_arrival_queue_.front().arrived_ns,
                                   last_right_activity_ns_);
        next = std::min(next, detail::saturating_add(base, timeout));
      }
      while (
        not right_arrival_queue_.empty()
        and not is_indexed_right(right_arrival_queue_.front().entry_id,
                                 right_arrival_queue_.front().generation)) {
        right_arrival_queue_.pop_front();
      }
      if (not right_arrival_queue_.empty()) {
        auto const base = std::max(right_arrival_queue_.front().arrived_ns,
                                   last_left_activity_ns_);
        next = std::min(next, detail::saturating_add(base, timeout));
      }
    }
    auto const prev
      = next_deadline_ns_.exchange(next, std::memory_order_acq_rel);
    if (next < prev) {
      wake_notify_.notify_one();
    }
  }

  auto maybe_finish_if_settled(OpCtx& ctx) -> Task<void> {
    if (phase_ == Phase::done or has_pending_work()) {
      co_return;
    }
    auto const left_done = left_eof_ and live_left_count_ == 0;
    auto const right_empty_early
      = right_eof_ and live_left_count_ == 0 and indexed_right_count_ == 0
        and (kind_ == JoinKind::inner or kind_ == JoinKind::semi);
    if (not left_done and not right_empty_early) {
      co_return;
    }
    maybe_warn_type_mismatch(ctx.dh());
    if (not right_eof_) {
      co_await cancel_right_sub(ctx);
    }
    next_deadline_ns_.store(no_deadline, std::memory_order_release);
    draining_output_.store(false, std::memory_order_release);
    clear_all_state();
    phase_ = Phase::done;
  }

  auto cancel_right_sub(OpCtx& ctx) -> Task<void> {
    right_discarding_.store(true, std::memory_order_release);
    right_send_cancel_.requestCancellation();
    right_rx_active_.store(false, std::memory_order_release);
    if (right_sub_spawned_) {
      if (auto sub = ctx.get_sub(SubKeyView{std::string_view{"right"}})) {
        co_await as<SubHandle<void>>(*sub).cancel();
      }
    }
  }

  auto abort_on_failure(OpCtx& ctx) -> Task<void> {
    phase_ = Phase::done;
    next_deadline_ns_.store(no_deadline, std::memory_order_release);
    draining_output_.store(false, std::memory_order_release);
    clear_all_state();
    co_await cancel_right_sub(ctx);
  }

  auto clear_all_state() -> void {
    pending_left_batch_ = None{};
    pending_right_batch_ = None{};
    pending_expiry_drain_ = false;
    left_pool_.clear();
    right_pool_.clear();
    key_to_bucket_.clear();
    buckets_.clear();
    free_bucket_ids_.clear();
    left_entries_.clear();
    free_left_ids_.clear();
    right_entries_.clear();
    free_right_ids_.clear();
    left_expiry_heap_.clear();
    right_expiry_heap_.clear();
    left_arrival_queue_.clear();
    right_arrival_queue_.clear();
    keys_bytes_ = 0;
    bucket_lists_bytes_ = 0;
    live_left_count_ = 0;
    indexed_right_count_ = 0;
  }

  auto tracked_bytes() const noexcept -> uint64_t {
    auto const table_bytes
      = static_cast<uint64_t>(key_to_bucket_.bucket_count())
        * (sizeof(JoinKey) + sizeof(uint32_t) + 8);
    auto const buckets_bytes
      = static_cast<uint64_t>(buckets_.capacity()) * sizeof(KeyBucket);
    auto const entries_bytes = detail::saturating_add(
      static_cast<uint64_t>(left_entries_.capacity()) * sizeof(LeftEntry),
      static_cast<uint64_t>(right_entries_.capacity()) * sizeof(RightEntry));
    auto const queue_bytes = detail::saturating_add(
      detail::saturating_add(left_expiry_heap_.capacity_bytes(),
                             right_expiry_heap_.capacity_bytes()),
      static_cast<uint64_t>(left_arrival_queue_.size()
                            + right_arrival_queue_.size())
        * sizeof(ArrivalItem));
    auto total = detail::saturating_add(left_pool_.approx_bytes(),
                                        right_pool_.approx_bytes());
    total = detail::saturating_add(total, keys_bytes_);
    total = detail::saturating_add(total, bucket_lists_bytes_);
    total = detail::saturating_add(total, table_bytes);
    total = detail::saturating_add(total, buckets_bytes);
    total = detail::saturating_add(total, entries_bytes);
    total = detail::saturating_add(total, queue_bytes);
    if (pending_left_batch_) {
      total = detail::saturating_add(
        total, static_cast<uint64_t>(pending_left_batch_->to_store.capacity())
                 * sizeof(PendingLeftStore));
    }
    if (pending_right_batch_) {
      auto const staged_map_bytes
        = static_cast<uint64_t>(
            pending_right_batch_->staged_best_for_left.bucket_count())
          * (sizeof(uint32_t) + sizeof(Candidate) + 8);
      auto const staged_vec_bytes
        = static_cast<uint64_t>(pending_right_batch_->to_store.capacity())
          * sizeof(PendingRightStore);
      total = detail::saturating_add(
        total, detail::saturating_add(staged_map_bytes, staged_vec_bytes));
    }
    return total;
  }

  auto fail_memory_limit(diagnostic_handler& dh) -> void {
    if (failed_) {
      return;
    }
    failed_ = true;
    diagnostic::error("`join` exceeded memory limit of {}",
                      format_bytes(max_memory_))
      .primary(memory_loc_)
      .hint("increase `max_memory`, narrow the time window, or filter the "
            "inputs")
      .emit(dh);
  }

  auto maybe_warn_type_mismatch(diagnostic_handler& dh) -> void {
    if (had_match_ or warned_type_mismatch_) {
      return;
    }
    warned_type_mismatch_ = true;
    warn_type_mismatch_if_disjoint(left_key_state_, right_key_state_, dh);
  }

  JoinArgs args_;
  ir::pipeline right_pipe_;
  JoinKind kind_ = JoinKind::inner;
  PickMode pick_ = PickMode::all;
  bool store_payloads_ = true;
  bool event_time_ = false;
  int64_t before_ns_ = 0;
  int64_t after_ns_ = 0;
  int64_t tolerance_ns_ = 0;
  Option<int64_t> idle_timeout_ns_;
  uint64_t max_memory_ = default_max_memory;
  location memory_loc_ = location::unknown;
  location left_ts_loc_ = location::unknown;
  location right_ts_loc_ = location::unknown;

  SideKeyState left_key_state_;
  SideKeyState right_key_state_;
  Option<nova::Evaluator> left_ts_eval_;
  Option<nova::Evaluator> right_ts_eval_;

  Phase phase_ = Phase::streaming;
  bool left_eof_ = false;
  bool right_eof_ = false;
  bool right_sub_spawned_ = false;
  bool failed_ = false;
  bool had_match_ = false;
  bool warned_type_mismatch_ = false;
  bool warned_invalid_left_ts_ = false;
  bool warned_invalid_right_ts_ = false;
  bool warned_late_left_ = false;
  bool warned_late_right_ = false;
  bool pending_expiry_drain_ = false;

  Option<int64_t> left_max_ts_;
  Option<int64_t> right_max_ts_;
  Option<int64_t> left_watermark_;
  Option<int64_t> right_watermark_;
  int64_t last_left_activity_ns_ = 0;
  int64_t last_right_activity_ns_ = 0;
  uint64_t next_right_seq_ = 0;
  uint64_t next_left_gen_ = 0;
  uint64_t next_right_gen_ = 0;

  Option<PendingLeftBatch> pending_left_batch_;
  Option<PendingRightBatch> pending_right_batch_;

  BatchPool<nova::Events> left_pool_;
  BatchPool<nova::Array<nova::Record>> right_pool_;
  tsl::robin_map<JoinKey, uint32_t, JoinKeyHash, JoinKeyEqual> key_to_bucket_;
  std::vector<KeyBucket> buckets_;
  std::vector<uint32_t> free_bucket_ids_;
  std::vector<LeftEntry> left_entries_;
  std::vector<uint32_t> free_left_ids_;
  std::vector<RightEntry> right_entries_;
  std::vector<uint32_t> free_right_ids_;
  ExpiryMinHeap left_expiry_heap_;
  ExpiryMinHeap right_expiry_heap_;
  std::deque<ArrivalItem> left_arrival_queue_;
  std::deque<ArrivalItem> right_arrival_queue_;
  uint64_t keys_bytes_ = 0;
  uint64_t bucket_lists_bytes_ = 0;
  size_t live_left_count_ = 0;
  size_t indexed_right_count_ = 0;

  Option<Sender<nova::Events>> right_tx_;
  mutable Option<Receiver<nova::Events>> right_rx_;
  mutable Notify wake_notify_;
  folly::CancellationSource right_send_cancel_;
  Atomic<bool> right_discarding_ = false;
  Atomic<bool> right_rx_active_ = false;
  Atomic<bool> draining_output_ = false;
  Atomic<int64_t> next_deadline_ns_ = no_deadline;
};

} // namespace

auto make_streaming_join(JoinArgs args, ir::pipeline right_pipe)
  -> Box<Operator<nova::Events, nova::Events>> {
  return Box<Operator<nova::Events, nova::Events>>{
    StreamingJoin{std::move(args), std::move(right_pipe)}.with_name("join")};
}

} // namespace tenzir::plugins::join
