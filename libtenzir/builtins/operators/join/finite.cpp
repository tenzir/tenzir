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
#include <tenzir/detail/narrow.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/concatenate.hpp>
#include <tenzir/nova/event_builder.hpp>
#include <tenzir/nova/materialize.hpp>

namespace tenzir::plugins::join {

auto build_key_states(std::vector<ast::expression>& keys,
                      location operator_location, OpCtx& ctx)
  -> Task<Option<std::pair<SideKeyState, SideKeyState>>> {
  if (keys.empty()) {
    diagnostic::error("missing join key")
      .primary(operator_location)
      .hint("specify one or more key expressions, e.g. `join id { ... }` or "
            "`join left_id == right_id { ... }`")
      .emit(ctx);
    co_return None{};
  }
  auto left_exprs = std::vector<ast::expression>{};
  auto right_exprs = std::vector<ast::expression>{};
  auto left_locs = std::vector<location>{};
  auto right_locs = std::vector<location>{};
  left_exprs.reserve(keys.size());
  right_exprs.reserve(keys.size());
  left_locs.reserve(keys.size());
  right_locs.reserve(keys.size());
  for (auto& key_expr : keys) {
    auto pair = classify_join_key(std::move(key_expr), ctx.reg(), ctx.dh());
    if (not pair) {
      co_return None{};
    }
    left_locs.push_back(pair->left_loc);
    right_locs.push_back(pair->right_loc);
    left_exprs.push_back(std::move(pair->left));
    right_exprs.push_back(std::move(pair->right));
  }
  keys.clear();
  auto left_evaluators = std::vector<nova::Evaluator>{};
  left_evaluators.reserve(left_exprs.size());
  for (auto& expr : left_exprs) {
    auto evaluator = co_await nova::Evaluator::make(std::move(expr), ctx);
    if (not evaluator) {
      co_return None{};
    }
    left_evaluators.push_back(std::move(*evaluator));
  }
  auto right_evaluators = std::vector<nova::Evaluator>{};
  right_evaluators.reserve(right_exprs.size());
  for (auto& expr : right_exprs) {
    auto evaluator = co_await nova::Evaluator::make(std::move(expr), ctx);
    if (not evaluator) {
      co_return None{};
    }
    right_evaluators.push_back(std::move(*evaluator));
  }
  auto left_state = SideKeyState{
    .evaluators = std::move(left_evaluators),
    .locations = std::move(left_locs),
    .observed_types = std::vector<uint32_t>(left_exprs.size(), 0),
  };
  auto right_state = SideKeyState{
    .evaluators = std::move(right_evaluators),
    .locations = std::move(right_locs),
    .observed_types = std::vector<uint32_t>(right_exprs.size(), 0),
  };
  co_return std::pair{std::move(left_state), std::move(right_state)};
}

auto BuildProbeState::next_fanout_chunk(BuildIndex const& index,
                                        PendingFanout& pending,
                                        diagnostic_handler& dh)
  -> Option<nova::Events> {
  if (pending.finished()) {
    return None{};
  }
  auto left_assembler = nova::EventsAssembler{};
  auto right_builder = nova::ArrayBuilder<nova::Data>{};
  auto emitted = nova::storage::Index{0};
  auto emitted_bytes = uint64_t{0};
  auto hit_byte_limit = false;
  while (emitted < output_chunk_rows and not hit_byte_limit
         and pending.row_pos < pending.rows.size()) {
    auto const& rm = pending.rows[pending.row_pos];
    auto const left_row_bytes
      = estimate_event_row_bytes(pending.input, rm.left_row);
    if (not rm.posting_idx) {
      if (emitted == 0 and left_row_bytes > max_single_row_bytes) {
        row_too_large = true;
        diagnostic::error("`join` matched row exceeds maximum output batch "
                          "size")
          .primary(operator_location)
          .emit(dh);
        return None{};
      }
      if (emitted > 0
          and detail::saturating_add(emitted_bytes, left_row_bytes)
                > output_chunk_bytes) {
        hit_byte_limit = true;
        break;
      }
      left_assembler.append(pending.input, rm.left_row);
      right_builder.null();
      emitted_bytes = detail::saturating_add(emitted_bytes, left_row_bytes);
      ++emitted;
      ++pending.row_pos;
      pending.match_pos = 0;
      continue;
    }
    auto const refs = index.posting(*rm.posting_idx);
    while (emitted < output_chunk_rows and pending.match_pos < refs.size()) {
      auto const& ref = refs[pending.match_pos];
      auto const right_view
        = nova::RowView<nova::Data>{index.batch(ref.batch).get(ref.row)};
      auto const row_bytes = detail::saturating_add(
        left_row_bytes, estimate_view_bytes(right_view));
      if (emitted == 0 and row_bytes > max_single_row_bytes) {
        row_too_large = true;
        diagnostic::error("`join` matched row exceeds maximum output batch "
                          "size")
          .primary(operator_location)
          .emit(dh);
        return None{};
      }
      if (emitted > 0
          and detail::saturating_add(emitted_bytes, row_bytes)
                > output_chunk_bytes) {
        hit_byte_limit = true;
        break;
      }
      ++pending.match_pos;
      left_assembler.append(pending.input, rm.left_row);
      nova::append_row(right_builder, right_view);
      emitted_bytes = detail::saturating_add(emitted_bytes, row_bytes);
      ++emitted;
    }
    if (not hit_byte_limit and pending.match_pos >= refs.size()) {
      ++pending.row_pos;
      pending.match_pos = 0;
    }
  }
  if (emitted == 0) {
    return None{};
  }
  auto left_events = std::move(left_assembler).finish();
  auto out_mask = left_events.mask;
  return combine_records(std::move(left_events), right_builder.finish(),
                         std::move(out_mask), into, dh);
}

auto BuildProbeState::probe_batch(nova::Events input, BuildIndex const& index,
                                  Push<nova::Events>& push,
                                  diagnostic_handler& dh) -> Task<bool> {
  if (not input.mask.any()) {
    co_return false;
  }
  TENZIR_ASSERT(not pending_fanout);
  // Fast path when the right-hand side had no eligible rows.
  if (index.empty()) {
    if (kind == JoinKind::inner or kind == JoinKind::semi) {
      co_return false;
    }
    if (kind == JoinKind::anti) {
      co_await push(std::move(input));
      co_return false;
    }
    TENZIR_ASSERT(kind == JoinKind::left);
    auto out_mask = input.mask;
    auto right_col = nova::Array<nova::Data>{
      nova::Array<nova::Null>{nova::storage::NullStorage{input.length()}}};
    co_await push(combine_records(std::move(input), std::move(right_col),
                                  std::move(out_mask), into, dh));
    co_return false;
  }
  auto eval_result = evaluate_keys(left_key_state, input, dh);
  if (kind == JoinKind::semi or kind == JoinKind::anti) {
    auto out_mask = nova::storage::BitMap::Mutable{
      nova::storage::BitMap{input.length(), false}};
    if (kind == JoinKind::anti) {
      auto const ineligible = input.mask.and_not(eval_result.eligible_mask);
      for (auto row : nova::storage::true_bits(ineligible)) {
        out_mask.set(row, true);
      }
    }
    auto key_view = JoinKeyView{};
    key_view.reserve(eval_result.columns.size());
    auto hash_idx = size_t{0};
    for (auto row : nova::storage::true_bits(eval_result.eligible_mask)) {
      auto const h = eval_result.hashes[hash_idx++];
      key_view.clear();
      for (auto const& col : eval_result.columns) {
        key_view.push_back(col.get(row));
      }
      auto const matched = index.contains(key_view, h);
      if (matched) {
        had_match = true;
      }
      if ((kind == JoinKind::semi and matched)
          or (kind == JoinKind::anti and not matched)) {
        out_mask.set(row, true);
      }
    }
    auto final_mask = std::move(out_mask).finish();
    if (final_mask.any()) {
      co_await push(nova::Events{std::move(input.data), std::move(final_mask),
                                 std::move(input.meta)});
    }
    co_return false;
  }
  auto row_matches = std::vector<RowMatches>{};
  row_matches.reserve(detail::narrow<size_t>(input.active_count()));
  auto key_view = JoinKeyView{};
  key_view.reserve(eval_result.columns.size());
  auto hash_idx = size_t{0};
  auto needs_chunking = false;
  auto estimated_right_bytes = uint64_t{0};
  for (auto row : nova::storage::true_bits(input.mask)) {
    auto posting = Option<size_t>{};
    if (eval_result.eligible_mask.get(row)) {
      auto const h = eval_result.hashes[hash_idx++];
      key_view.clear();
      for (auto const& col : eval_result.columns) {
        key_view.push_back(col.get(row));
      }
      posting = index.find_posting(key_view, h);
    }
    if (posting) {
      auto const refs = index.posting(*posting);
      had_match = true;
      if (refs.size() > 1) {
        needs_chunking = true;
      } else if (not needs_chunking) {
        auto const ref = refs.front();
        auto const row_bytes = estimate_view_bytes(
          nova::RowView<nova::Data>{index.batch(ref.batch).get(ref.row)});
        estimated_right_bytes
          = detail::saturating_add(estimated_right_bytes, row_bytes);
        if (estimated_right_bytes > output_chunk_bytes) {
          needs_chunking = true;
        }
      }
      row_matches.push_back(RowMatches{row, posting});
    } else if (kind == JoinKind::left) {
      row_matches.push_back(RowMatches{row, None{}});
    }
  }
  if (row_matches.empty()) {
    co_return false;
  }
  if (not needs_chunking and input.length() <= output_chunk_rows
      and detail::narrow<nova::storage::Index>(row_matches.size())
            <= output_chunk_rows) {
    auto out_mask = kind == JoinKind::left
                      ? input.mask
                      : nova::storage::BitMap{input.length(), false};
    auto matched_mask_builder = Option<nova::storage::BitMap::Mutable>{};
    if (kind == JoinKind::inner) {
      matched_mask_builder.emplace(
        nova::storage::BitMap{input.length(), false});
    }
    auto right_builder = nova::ArrayBuilder<nova::Data>{};
    for (auto const& rm : row_matches) {
      right_builder.skip_n(rm.left_row - right_builder.length());
      if (rm.posting_idx) {
        auto const ref = index.posting(*rm.posting_idx).front();
        nova::append_row(right_builder, nova::RowView<nova::Data>{
                                          index.batch(ref.batch).get(ref.row)});
        if (matched_mask_builder) {
          matched_mask_builder->set(rm.left_row, true);
        }
      } else {
        right_builder.null();
      }
    }
    right_builder.skip_n(input.length() - right_builder.length());
    if (matched_mask_builder) {
      out_mask = std::move(*matched_mask_builder).finish();
    }
    TENZIR_ASSERT(out_mask.any());
    co_await push(combine_records(std::move(input), right_builder.finish(),
                                  std::move(out_mask), into, dh));
    co_return false;
  }
  auto pending = PendingFanout{
    .input = std::move(input),
    .rows = std::move(row_matches),
    .row_pos = 0,
    .match_pos = 0,
  };
  if (auto chunk = next_fanout_chunk(index, pending, dh)) {
    co_await push(std::move(*chunk));
  }
  if (row_too_large or pending.finished()) {
    co_return false;
  }
  pending_fanout = std::move(pending);
  co_return true;
}

auto BuildProbeState::drain_one_chunk(BuildIndex const& index,
                                      Push<nova::Events>& push,
                                      diagnostic_handler& dh) -> Task<bool> {
  if (not pending_fanout) {
    co_return false;
  }
  if (auto chunk = next_fanout_chunk(index, *pending_fanout, dh)) {
    co_await push(std::move(*chunk));
  }
  if (row_too_large or pending_fanout->finished()) {
    pending_fanout = None{};
    co_return false;
  }
  co_return true;
}

namespace {

class FiniteJoin final : public Operator<nova::Events, nova::Events> {
public:
  FiniteJoin(JoinArgs args, ir::pipeline right_pipe)
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
    auto kind = JoinKind::inner;
    if (args_.kind) {
      auto parsed = parse_kind(args_.kind->inner);
      TENZIR_ASSERT(parsed);
      kind = *parsed;
    }
    auto states
      = co_await build_key_states(args_.keys, args_.operator_location, ctx);
    if (not states) {
      phase_ = Phase::done;
      co_return;
    }
    auto& [left_state, right_state] = *states;
    probe_.kind = kind;
    probe_.into = args_.into;
    probe_.operator_location = args_.operator_location;
    probe_.left_key_state = std::move(left_state);
    auto const store_payloads
      = kind == JoinKind::inner or kind == JoinKind::left;
    auto const max_memory
      = args_.max_memory ? args_.max_memory->inner : default_max_memory;
    auto const memory_loc
      = args_.max_memory ? args_.max_memory->source : args_.operator_location;
    build_index_.emplace(std::move(right_state), store_payloads, max_memory,
                         memory_loc);
    phase_ = Phase::building;
    auto sub = co_await ctx.plan_and_spawn_sub<void>(data{"right"},
                                                     std::move(right_pipe_));
    if (not sub) {
      phase_ = Phase::done;
      build_index_ = None{};
      co_return;
    }
    co_return;
  }

  auto state() -> OperatorState override {
    switch (phase_) {
      case Phase::building:
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
    TENZIR_UNUSED(key, push);
    if (stopped_during_build_.load(std::memory_order_acquire)
        or build_memory_exceeded_.load(std::memory_order_acquire)) {
      co_return;
    }
    TENZIR_ASSERT(build_index_);
    build_index_->add(std::move(events), ctx.dh());
    if (build_index_->memory_exceeded()) {
      build_memory_exceeded_.store(true, std::memory_order_release);
      drain_notify_.notify_one();
    }
    co_return;
  }

  auto finish_sub(SubKeyView key, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(key, push, ctx);
    if (phase_ != Phase::building) {
      if (build_index_) {
        build_index_->clear();
      }
      co_return;
    }
    TENZIR_ASSERT(build_index_);
    if (build_index_->memory_exceeded()) {
      phase_ = Phase::done;
      build_index_->clear();
      co_return;
    }
    if (build_index_->empty()
        and (probe_.kind == JoinKind::inner or probe_.kind == JoinKind::semi)) {
      phase_ = Phase::done;
      build_index_->clear();
      co_return;
    }
    phase_ = Phase::probing;
    co_return;
  }

  auto finish_sub(SubKeyView key, failure error, Push<nova::Events>& push,
                  OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(key, error, push, ctx);
    phase_ = Phase::done;
    if (build_index_) {
      build_index_->clear();
    }
    co_return;
  }

  auto stop(OpCtx& ctx) -> Task<void> override {
    if (phase_ == Phase::building) {
      stopped_during_build_.store(true, std::memory_order_release);
      phase_ = Phase::done;
      if (not build_memory_exceeded_.load(std::memory_order_acquire)) {
        diagnostic::error(
          "`join` interrupted before the right-hand pipeline finished")
          .primary(args_.operator_location)
          .emit(ctx);
      }
    }
    co_return;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    co_await drain_notify_.wait();
    co_return DrainTick{};
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not result.try_as<DrainTick>()) {
      co_return;
    }
    if (phase_ == Phase::building
        and build_memory_exceeded_.load(std::memory_order_acquire)) {
      phase_ = Phase::done;
      co_return;
    }
    if (phase_ != Phase::draining_fanout or not build_index_) {
      co_return;
    }
    auto more = co_await probe_.drain_one_chunk(*build_index_, push, ctx.dh());
    if (probe_.row_too_large) {
      phase_ = Phase::done;
      co_return;
    }
    if (not more) {
      phase_ = Phase::probing;
      co_return;
    }
    drain_notify_.notify_one();
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (phase_ != Phase::probing or not input.mask.any()) {
      co_return;
    }
    TENZIR_ASSERT(build_index_);
    auto more = co_await probe_.probe_batch(std::move(input), *build_index_,
                                            push, ctx.dh());
    if (probe_.row_too_large) {
      phase_ = Phase::done;
      co_return;
    }
    if (more) {
      phase_ = Phase::draining_fanout;
      drain_notify_.notify_one();
    }
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(push);
    if (phase_ == Phase::done or not build_index_) {
      co_return FinalizeBehavior::done;
    }
    TENZIR_ASSERT(not probe_.pending_fanout);
    if (not probe_.had_match) {
      warn_type_mismatch_if_disjoint(probe_.left_key_state,
                                     build_index_->key_state(), ctx.dh());
    }
    co_return FinalizeBehavior::done;
  }

private:
  enum class Phase : uint8_t {
    building,
    probing,
    draining_fanout,
    done,
  };

  struct DrainTick {};

  JoinArgs args_;
  ir::pipeline right_pipe_;
  Phase phase_ = Phase::building;
  BuildProbeState probe_;
  Option<BuildIndex> build_index_;
  mutable Notify drain_notify_;
  Atomic<bool> build_memory_exceeded_ = false;
  Atomic<bool> stopped_during_build_ = false;
};

} // namespace

auto make_finite_join(JoinArgs args, ir::pipeline right_pipe)
  -> Box<Operator<nova::Events, nova::Events>> {
  return Box<Operator<nova::Events, nova::Events>>{
    FiniteJoin{std::move(args), std::move(right_pipe)}.with_name("join")};
}

} // namespace tenzir::plugins::join
