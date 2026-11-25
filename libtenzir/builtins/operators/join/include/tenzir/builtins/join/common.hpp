//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/async.hpp>
#include <tenzir/detail/saturating_arithmetic.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/hash/hash.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova/union_array.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/tql2/ast.hpp>

#include <tsl/robin_map.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::plugins::join {

using namespace tenzir::si_literals;

inline constexpr auto default_max_memory = uint64_t{512_Mi};
inline constexpr auto output_chunk_rows = nova::storage::Index{16'384};
inline constexpr auto output_chunk_bytes = uint64_t{64_Mi};
inline constexpr auto max_single_row_bytes
  = static_cast<uint64_t>(std::numeric_limits<nova::storage::Index>::max() / 2);

enum class JoinKind : uint8_t {
  inner,
  left,
  semi,
  anti,
};

enum class PickMode : uint8_t {
  all,
  latest,
  earliest,
  nearest,
};

auto parse_kind(std::string_view value) -> Option<JoinKind>;
auto kind_name(JoinKind kind) -> std::string_view;
auto parse_pick(std::string_view value) -> Option<PickMode>;
auto format_bytes(uint64_t bytes) -> std::string;

struct JoinKeyPair {
  ast::expression left;
  ast::expression right;
  location left_loc;
  location right_loc;
};

auto classify_join_key(ast::expression expr, registry const& reg,
                       diagnostic_handler& dh) -> failure_or<JoinKeyPair>;

auto classify_timestamp_expr(ast::expression expr, registry const& reg,
                             diagnostic_handler& dh) -> failure_or<JoinKeyPair>;

struct JoinArgs {
  location operator_location;
  std::vector<ast::expression> keys;
  Option<located<std::string>> kind;
  Option<ast::field_path> into;
  Option<located<uint64_t>> max_memory;
  Option<located<duration>> refresh;
  Option<located<duration>> before;
  Option<located<duration>> after;
  Option<located<duration>> within;
  Option<ast::expression> on;
  Option<located<duration>> tolerance;
  Option<located<duration>> idle_timeout;
  Option<located<std::string>> pick;

  auto is_streaming() const noexcept -> bool {
    return before.has_value() or after.has_value() or within.has_value();
  }

  auto is_refresh() const noexcept -> bool {
    return refresh.has_value();
  }
};

struct JoinKey : std::vector<nova::Data> {
  using vector::vector;
};

struct JoinKeyView : std::vector<nova::RowView<nova::Data>> {
  using vector::vector;
};

template <class Key>
concept JoinKeyType = concepts::one_of<Key, JoinKey, JoinKeyView>;

struct JoinKeyHash {
  using is_transparent = void;

  /// Combines per-component hashes identically to `nova::hash_rows`, so
  /// precomputed column hashes can probe the hash table directly.
  template <JoinKeyType Key>
  auto operator()(Key const& key) const noexcept -> uint64_t {
    auto result = uint64_t{0};
    for (auto const& value : key) {
      result = tenzir::hash(result, static_cast<uint64_t>(nova::hash(
                                      nova::RowView<nova::Data>{value})));
    }
    return result;
  }
};

struct JoinKeyEqual {
  using is_transparent = void;

  /// Every key stored in or probed against the hash table is guaranteed to
  /// contain no `NaN` and no `Secret`, so `nova::equal` is reflexive and
  /// consistent with `JoinKeyHash`.
  template <JoinKeyType Lhs, JoinKeyType Rhs>
  auto operator()(Lhs const& lhs, Rhs const& rhs) const -> bool {
    return std::ranges::equal(lhs, rhs, [](auto const& x, auto const& y) {
      return nova::equal(nova::RowView<nova::Data>{x},
                         nova::RowView<nova::Data>{y});
    });
  }
};

auto materialize_key(JoinKeyView const& views) -> JoinKey;
auto estimate_view_bytes(nova::RowView<nova::Data> const& value) -> uint64_t;
auto estimate_key_heap_bytes(JoinKey const& key) -> uint64_t;
auto estimate_key_view_heap_bytes(JoinKeyView const& views) -> uint64_t;
auto estimate_event_row_bytes(nova::Events const& events,
                              nova::storage::Index row) -> uint64_t;

struct SideKeyState {
  std::vector<nova::Evaluator> evaluators;
  std::vector<location> locations;
  std::vector<uint32_t> observed_types;
  bool warned_secret = false;
};

struct KeyEvalResult {
  std::vector<nova::Array<nova::Data>> columns;
  nova::storage::BitMap eligible_mask;
  std::vector<uint64_t> hashes;
};

auto evaluate_keys(SideKeyState& side, nova::Events const& events,
                   diagnostic_handler& dh) -> KeyEvalResult;

auto format_type_mask(uint32_t mask) -> std::pair<std::string, bool>;

auto warn_type_mismatch_if_disjoint(SideKeyState const& left_state,
                                    SideKeyState const& right_state,
                                    diagnostic_handler& dh) -> void;

auto combine_records(nova::Events left, nova::Array<nova::Data> right_col,
                     nova::storage::BitMap out_mask,
                     Option<ast::field_path> const& into,
                     diagnostic_handler& dh) -> nova::Events;

struct RowRef {
  uint32_t batch = 0;
  nova::storage::Index row = 0;
};

class BuildIndex {
public:
  BuildIndex(SideKeyState key_state, bool store_payloads, uint64_t max_memory,
             location memory_loc, bool warn_on_limit = false,
             uint64_t reserved_bytes = 0);

  auto add(nova::Events events, diagnostic_handler& dh) -> void;
  auto clear() -> void;
  auto set_reserved_bytes(uint64_t reserved) noexcept -> void {
    reserved_bytes_ = reserved;
  }

  auto memory_exceeded() const noexcept -> bool {
    return memory_exceeded_;
  }

  auto empty() const noexcept -> bool {
    return map_.empty();
  }

  auto contains(JoinKeyView const& key, uint64_t hash) const -> bool {
    return map_.find(key, hash) != map_.end();
  }

  auto find_posting(JoinKeyView const& key, uint64_t hash) const
    -> Option<size_t> {
    auto it = map_.find(key, hash);
    if (it == map_.end()) {
      return None{};
    }
    return it->second;
  }

  auto posting(size_t index) const -> std::span<RowRef const> {
    return postings_[index];
  }

  auto batch(uint32_t index) const -> nova::Array<nova::Record> const& {
    return batches_[index];
  }

  auto key_state() const -> SideKeyState const& {
    return key_state_;
  }

  auto approx_bytes() const noexcept -> uint64_t;

private:
  auto fail_memory_limit(diagnostic_handler& dh) -> void;

  SideKeyState key_state_;
  bool store_payloads_ = true;
  bool warn_on_limit_ = false;
  uint64_t max_memory_ = default_max_memory;
  uint64_t reserved_bytes_ = 0;
  location memory_loc_ = location::unknown;
  std::vector<nova::Array<nova::Record>> batches_;
  tsl::robin_map<JoinKey, size_t, JoinKeyHash, JoinKeyEqual> map_;
  std::vector<std::vector<RowRef>> postings_;
  uint64_t batches_bytes_ = 0;
  uint64_t keys_bytes_ = 0;
  uint64_t postings_bytes_ = 0;
  bool memory_exceeded_ = false;
};

struct RowMatches {
  nova::storage::Index left_row = 0;
  Option<size_t> posting_idx;
};

struct PendingFanout {
  nova::Events input;
  std::vector<RowMatches> rows;
  size_t row_pos = 0;
  size_t match_pos = 0;

  auto finished() const noexcept -> bool {
    return row_pos >= rows.size();
  }
};

/// Shared probe & chunked fan-out engine for both finite (`BuildIndex`) and
/// periodic snapshot (`refresh=`) joins.
struct BuildProbeState {
  JoinKind kind = JoinKind::inner;
  Option<ast::field_path> into;
  location operator_location = location::unknown;
  SideKeyState left_key_state;
  Option<PendingFanout> pending_fanout;
  bool had_match = false;
  bool row_too_large = false;

  auto next_fanout_chunk(BuildIndex const& index, PendingFanout& pending,
                         diagnostic_handler& dh) -> Option<nova::Events>;

  /// Probes `input` against `index`, pushing the first (or only) chunk.
  /// Returns `true` if `pending_fanout` has more chunks to drain via
  /// `drain_one_chunk()`.
  auto probe_batch(nova::Events input, BuildIndex const& index,
                   Push<nova::Events>& push, diagnostic_handler& dh)
    -> Task<bool>;

  /// Emits one chunk from `pending_fanout`. Returns `true` if more chunks
  /// remain.
  auto drain_one_chunk(BuildIndex const& index, Push<nova::Events>& push,
                       diagnostic_handler& dh) -> Task<bool>;
};

auto build_key_states(std::vector<ast::expression>& keys,
                      location operator_location, OpCtx& ctx)
  -> Task<Option<std::pair<SideKeyState, SideKeyState>>>;

auto make_finite_join(JoinArgs args, ir::pipeline right_pipe)
  -> Box<Operator<nova::Events, nova::Events>>;

/// `right_projection` is the projection hint that `JoinIr::optimize` computed
/// for the right-hand subpipeline. Refresh joins plan the subpipeline anew on
/// every reload, so they apply the hint at that point.
auto make_refresh_join(JoinArgs args, ir::pipeline right_pipe,
                       Option<ir::OptimizeProjection> right_projection)
  -> Box<Operator<nova::Events, nova::Events>>;

auto make_streaming_join(JoinArgs args, ir::pipeline right_pipe)
  -> Box<Operator<nova::Events, nova::Events>>;

} // namespace tenzir::plugins::join
