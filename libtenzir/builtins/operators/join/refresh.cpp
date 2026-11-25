//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/async/notify.hpp>
#include <tenzir/atomic.hpp>
#include <tenzir/builtins/join/common.hpp>

#include <folly/coro/Collect.h>

#include <chrono>
#include <limits>

namespace tenzir::plugins::join {

namespace {

inline constexpr auto no_deadline = std::numeric_limits<int64_t>::max();

/// Demotes `severity::error` diagnostics to `severity::warning` during
/// `ir::make_plan` on periodic refresh ticks so transient planning errors
/// (such as a missing file during rotation) retain the previous snapshot.
class RefreshPlanDiagHandler final : public diagnostic_handler {
public:
  explicit RefreshPlanDiagHandler(diagnostic_handler& parent)
    : parent_{parent} {
  }

  auto emit(diagnostic d) -> void override {
    if (d.severity == severity::error) {
      d.severity = severity::warning;
      d.notes.emplace_back(diagnostic_note_kind::note,
                           "keeping the previous snapshot");
    }
    parent_.emit(std::move(d));
  }

private:
  diagnostic_handler& parent_;
};

class RefreshJoin final : public Operator<nova::Events, nova::Events> {
public:
  RefreshJoin(JoinArgs args, ir::pipeline right_pipe,
              Option<ir::OptimizeProjection> right_projection)
    : args_{std::move(args)},
      right_pipe_{std::move(right_pipe)},
      right_projection_{std::move(right_projection)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (ctx.checkpoint_settings()) {
      phase_ = Phase::done;
      diagnostic::error("`join` does not support checkpointing yet")
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    TENZIR_ASSERT(args_.refresh);
    refresh_interval_ = args_.refresh->inner;
    auto kind = JoinKind::inner;
    if (args_.kind) {
      auto parsed = parse_kind(args_.kind->inner);
      TENZIR_ASSERT(parsed);
      kind = *parsed;
    }
    if (args_.keys.empty()) {
      phase_ = Phase::done;
      diagnostic::error("missing join key")
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    auto left_exprs = std::vector<ast::expression>{};
    auto left_locs = std::vector<location>{};
    left_exprs.reserve(args_.keys.size());
    left_locs.reserve(args_.keys.size());
    right_key_exprs_.reserve(args_.keys.size());
    right_key_locs_.reserve(args_.keys.size());
    for (auto& key_expr : args_.keys) {
      auto pair = classify_join_key(std::move(key_expr), ctx.reg(), ctx.dh());
      if (not pair) {
        phase_ = Phase::done;
        co_return;
      }
      left_locs.push_back(pair->left_loc);
      right_key_locs_.push_back(pair->right_loc);
      left_exprs.push_back(std::move(pair->left));
      right_key_exprs_.push_back(std::move(pair->right));
    }
    args_.keys.clear();
    auto left_evaluators = std::vector<nova::Evaluator>{};
    left_evaluators.reserve(left_exprs.size());
    for (auto& expr : left_exprs) {
      auto evaluator = co_await nova::Evaluator::make(std::move(expr), ctx);
      if (not evaluator) {
        phase_ = Phase::done;
        co_return;
      }
      left_evaluators.push_back(std::move(*evaluator));
    }
    probe_.kind = kind;
    probe_.into = args_.into;
    probe_.operator_location = args_.operator_location;
    probe_.left_key_state = SideKeyState{
      .evaluators = std::move(left_evaluators),
      .locations = std::move(left_locs),
      .observed_types = std::vector<uint32_t>(left_exprs.size(), 0),
    };
    store_payloads_ = kind == JoinKind::inner or kind == JoinKind::left;
    max_memory_
      = args_.max_memory ? args_.max_memory->inner : default_max_memory;
    memory_loc_
      = args_.max_memory ? args_.max_memory->source : args_.operator_location;
    phase_ = Phase::initial_build;
    auto ok
      = co_await start_build(ctx, /*is_initial=*/true, /*reserved_bytes=*/0);
    if (not ok) {
      phase_ = Phase::done;
      co_return;
    }
  }

  auto state() -> OperatorState override {
    switch (phase_) {
      case Phase::initial_build:
      case Phase::draining_fanout:
        return OperatorState::blocked;
      case Phase::probing:
        return OperatorState::normal;
      case Phase::done:
        return OperatorState::done;
    }
    TENZIR_UNREACHABLE();
  }

  auto process_sub(SubKeyView key, nova::Events events,
                   Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push);
    if (stopping_.load(std::memory_order_acquire)
        or shadow_limit_hit_.load(std::memory_order_acquire)) {
      co_return;
    }
    auto const* gen = try_as<uint64_t>(key);
    if (not gen or *gen != shadow_gen_.load(std::memory_order_acquire)) {
      co_return;
    }
    TENZIR_ASSERT(shadow_index_);
    shadow_index_->add(std::move(events), ctx.dh());
    if (shadow_index_->memory_exceeded()) {
      shadow_limit_hit_.store(true, std::memory_order_release);
      wake_notify_.notify_one();
    }
    co_return;
  }

  auto finish_sub(SubKeyView key, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(push, ctx);
    auto const* gen = try_as<uint64_t>(key);
    if (not gen or *gen != shadow_gen_.load(std::memory_order_relaxed)) {
      co_return;
    }
    shadow_in_flight_ = false;
    auto const hit_limit = shadow_limit_hit_.load(std::memory_order_acquire);
    shadow_limit_hit_.store(false, std::memory_order_release);
    if (phase_ == Phase::initial_build) {
      if (not shadow_index_ or shadow_index_->memory_exceeded() or hit_limit) {
        phase_ = Phase::done;
        shadow_index_ = None{};
        co_return;
      }
      shadow_index_->set_reserved_bytes(0);
      active_index_ = std::move(shadow_index_);
      shadow_index_ = None{};
      phase_ = Phase::probing;
      schedule_next_refresh();
      co_return;
    }
    if (phase_ == Phase::done) {
      shadow_index_ = None{};
      co_return;
    }
    if (shadow_index_ and not shadow_index_->memory_exceeded()
        and not hit_limit) {
      shadow_index_->set_reserved_bytes(0);
      if (probe_.pending_fanout) {
        staged_index_ = std::move(shadow_index_);
      } else {
        active_index_ = std::move(shadow_index_);
      }
    }
    shadow_index_ = None{};
    if (not stopping_.load(std::memory_order_relaxed)) {
      schedule_next_refresh();
    }
    co_return;
  }

  auto finish_sub(SubKeyView key, failure error, Push<nova::Events>& push,
                  OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(error, push, ctx);
    auto const* gen = try_as<uint64_t>(key);
    if (not gen or *gen != shadow_gen_.load(std::memory_order_relaxed)) {
      co_return;
    }
    shadow_in_flight_ = false;
    shadow_limit_hit_.store(false, std::memory_order_release);
    shadow_index_ = None{};
    if (phase_ == Phase::initial_build) {
      phase_ = Phase::done;
      co_return;
    }
    if (phase_ != Phase::done
        and not stopping_.load(std::memory_order_relaxed)) {
      schedule_next_refresh();
    }
    co_return;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    stopping_.store(true, std::memory_order_release);
    next_deadline_ns_.store(no_deadline, std::memory_order_release);
    wake_notify_.notify_one();
    if (phase_ == Phase::initial_build) {
      phase_ = Phase::done;
      if (not shadow_limit_hit_.load(std::memory_order_acquire)) {
        diagnostic::error(
          "`join` interrupted before the right-hand pipeline finished")
          .primary(args_.operator_location)
          .emit(ctx);
      }
      co_return;
    }
    if (shadow_in_flight_) {
      co_await cancel_shadow_sub(ctx);
    }
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    auto const deadline_ns = next_deadline_ns_.load(std::memory_order_acquire);
    if (deadline_ns == no_deadline) {
      co_await wake_notify_.wait();
      co_return WakeTick{};
    }
    auto const deadline = std::chrono::steady_clock::time_point{
      std::chrono::nanoseconds{deadline_ns}};
    auto [wake, timer] = co_await folly::coro::collectAnyNoDiscard(
      wake_notify_.wait(), sleep_until(deadline));
    if (wake.hasValue()) {
      co_return WakeTick{};
    }
    timer.value();
    co_return RefreshTick{};
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (phase_ == Phase::done) {
      co_return;
    }
    if (shadow_in_flight_
        and shadow_limit_hit_.load(std::memory_order_acquire)) {
      if (phase_ == Phase::initial_build) {
        phase_ = Phase::done;
        next_deadline_ns_.store(no_deadline, std::memory_order_release);
        co_await cancel_shadow_sub(ctx);
        co_return;
      }
      // `cancel()` is idempotent, so repeated wakeups while the cancelled
      // shadow build drains are harmless.
      co_await cancel_shadow_sub(ctx);
    }
    if (phase_ == Phase::draining_fanout and active_index_) {
      auto more
        = co_await probe_.drain_one_chunk(*active_index_, push, ctx.dh());
      if (probe_.row_too_large) {
        phase_ = Phase::done;
        next_deadline_ns_.store(no_deadline, std::memory_order_release);
        if (shadow_in_flight_) {
          co_await cancel_shadow_sub(ctx);
        }
        co_return;
      }
      if (not more) {
        if (staged_index_) {
          active_index_ = std::move(staged_index_);
          staged_index_ = None{};
        }
        phase_ = Phase::probing;
      } else {
        wake_notify_.notify_one();
      }
    }
    auto const want_refresh = result.try_as<RefreshTick>() or refresh_deferred_;
    if (want_refresh and not shadow_in_flight_
        and not stopping_.load(std::memory_order_relaxed)
        and (phase_ == Phase::probing or phase_ == Phase::draining_fanout)) {
      if (staged_index_) {
        // Never hold three snapshot generations simultaneously (`active` +
        // `staged` + `shadow`); wait until the current fan-out drain installs
        // `staged_index_` before starting the next refresh.
        refresh_deferred_ = true;
        next_deadline_ns_.store(no_deadline, std::memory_order_release);
        co_return;
      }
      refresh_deferred_ = false;
      next_deadline_ns_.store(no_deadline, std::memory_order_release);
      auto const active_bytes
        = active_index_ ? active_index_->approx_bytes() : uint64_t{0};
      auto ok = co_await start_build(ctx, /*is_initial=*/false, active_bytes);
      if (not ok and not stopping_.load(std::memory_order_relaxed)) {
        schedule_next_refresh();
      }
    }
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (phase_ != Phase::probing or not input.mask.any()) {
      co_return;
    }
    TENZIR_ASSERT(active_index_);
    auto more = co_await probe_.probe_batch(std::move(input), *active_index_,
                                            push, ctx.dh());
    if (probe_.row_too_large) {
      phase_ = Phase::done;
      next_deadline_ns_.store(no_deadline, std::memory_order_release);
      if (shadow_in_flight_) {
        co_await cancel_shadow_sub(ctx);
      }
      co_return;
    }
    if (more) {
      phase_ = Phase::draining_fanout;
      wake_notify_.notify_one();
    }
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    stopping_.store(true, std::memory_order_release);
    next_deadline_ns_.store(no_deadline, std::memory_order_release);
    if (shadow_in_flight_) {
      co_await cancel_shadow_sub(ctx);
    }
    if (phase_ != Phase::done and active_index_ and not probe_.had_match) {
      warn_type_mismatch_if_disjoint(probe_.left_key_state,
                                     active_index_->key_state(), ctx.dh());
    }
    phase_ = Phase::done;
    co_return FinalizeBehavior::done;
  }

private:
  enum class Phase : uint8_t {
    initial_build,
    probing,
    draining_fanout,
    done,
  };

  struct WakeTick {};
  struct RefreshTick {};

  auto make_right_key_state(OpCtx& ctx) -> Task<Option<SideKeyState>> {
    auto evaluators = std::vector<nova::Evaluator>{};
    evaluators.reserve(right_key_exprs_.size());
    for (auto const& expr : right_key_exprs_) {
      auto evaluator = co_await nova::Evaluator::make(expr, ctx);
      if (not evaluator) {
        co_return None{};
      }
      evaluators.push_back(std::move(*evaluator));
    }
    co_return SideKeyState{
      .evaluators = std::move(evaluators),
      .locations = right_key_locs_,
      .observed_types = std::vector<uint32_t>(right_key_exprs_.size(), 0),
    };
  }

  auto start_build(OpCtx& ctx, bool is_initial, uint64_t reserved_bytes)
    -> Task<bool> {
    auto right_state = co_await make_right_key_state(ctx);
    if (not right_state) {
      co_return false;
    }
    auto const gen = shadow_gen_.load(std::memory_order_relaxed) + 1;
    shadow_limit_hit_.store(false, std::memory_order_release);
    shadow_index_.emplace(std::move(*right_state), store_payloads_, max_memory_,
                          memory_loc_, /*warn_on_limit=*/not is_initial,
                          reserved_bytes);
    shadow_gen_.store(gen, std::memory_order_release);
    shadow_in_flight_ = true;
    auto pipe_copy = right_pipe_;
    auto plan = failure_or<ir::Plan>{failure::promise()};
    if (is_initial) {
      plan = ir::make_plan(std::move(pipe_copy), tag_v<void>, ctx,
                           ir::parallelism::disabled, right_projection_);
    } else {
      auto demoting_dh = RefreshPlanDiagHandler{ctx.dh()};
      auto plan_ctx = base_ctx{demoting_dh, ctx.reg()};
      plan = ir::make_plan(std::move(pipe_copy), tag_v<void>, plan_ctx,
                           ir::parallelism::disabled, right_projection_);
    }
    if (not plan) {
      shadow_in_flight_ = false;
      shadow_index_ = None{};
      co_return false;
    }
    auto const diag_behavior = is_initial ? DiagnosticBehavior::Unchanged
                                          : DiagnosticBehavior::ErrorToWarning;
    co_await ctx.spawn_sub(data{gen}, std::move(*plan), diag_behavior);
    co_return true;
  }

  auto cancel_shadow_sub(OpCtx& ctx) -> Task<void> {
    auto const gen = shadow_gen_.load(std::memory_order_relaxed);
    if (auto sub = ctx.get_sub(SubKeyView{gen})) {
      co_await as<SubHandle<void>>(*sub).cancel();
    }
  }

  auto schedule_next_refresh() -> void {
    auto const next = std::chrono::steady_clock::now() + refresh_interval_;
    auto const ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      next.time_since_epoch())
                      .count();
    next_deadline_ns_.store(ns, std::memory_order_release);
    wake_notify_.notify_one();
  }

  JoinArgs args_;
  ir::pipeline right_pipe_;
  Option<ir::OptimizeProjection> right_projection_;
  duration refresh_interval_{};
  std::vector<ast::expression> right_key_exprs_;
  std::vector<location> right_key_locs_;
  bool store_payloads_ = true;
  uint64_t max_memory_ = default_max_memory;
  location memory_loc_ = location::unknown;

  Phase phase_ = Phase::initial_build;
  BuildProbeState probe_;
  Option<BuildIndex> active_index_;
  Option<BuildIndex> staged_index_;
  Option<BuildIndex> shadow_index_;

  bool shadow_in_flight_ = false;
  bool refresh_deferred_ = false;

  mutable Notify wake_notify_;
  Atomic<uint64_t> shadow_gen_ = 0;
  Atomic<bool> shadow_limit_hit_ = false;
  Atomic<bool> stopping_ = false;
  Atomic<int64_t> next_deadline_ns_ = no_deadline;
};

} // namespace

auto make_refresh_join(JoinArgs args, ir::pipeline right_pipe,
                       Option<ir::OptimizeProjection> right_projection)
  -> Box<Operator<nova::Events, nova::Events>> {
  return Box<Operator<nova::Events, nova::Events>>{RefreshJoin{
    std::move(args), std::move(right_pipe), std::move(right_projection)}
                                                     .with_name("join")};
}

} // namespace tenzir::plugins::join
