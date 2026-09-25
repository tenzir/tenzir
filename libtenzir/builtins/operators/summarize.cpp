//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/bounded_queue.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/box.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/saturating_arithmetic.hpp>
#include <tenzir/error.hpp>
#include <tenzir/hash/hash.hpp>
#include <tenzir/hash/hash_append.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/multi_series.hpp>
#include <tenzir/nova/aggregation.hpp>
#include <tenzir/nova/array_builder.hpp>
#include <tenzir/nova/bitmap_iteration.hpp>
#include <tenzir/nova/eval.hpp>
#include <tenzir/nova/eval_util.hpp>
#include <tenzir/nova/events.hpp>
#include <tenzir/nova/union_array.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/session.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/registry.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/type.hpp>

#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <folly/coro/BoundedQueue.h>
#include <tsl/robin_map.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <limits>
#include <numeric>
#include <ranges>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace tenzir::plugins::summarize {

namespace {

using std::chrono::steady_clock;

/// The key by which aggregations are grouped. Essentially, this is a vector of
/// data. We create a new type here to support a custom hash and equality
/// operation to support lookups with non-materialized keys.
struct GroupKey : std::vector<data> {
  using vector::vector;
};

/// A view on a group-by key.
struct GroupKeyView : std::vector<data_view3> {
  using vector::vector;

  /// Materializes a view on a group-by key.
  friend auto materialize(GroupKeyView const& views) -> GroupKey {
    auto result = GroupKey{};
    result.reserve(views.size());
    for (auto const& view : views) {
      result.push_back(materialize(view));
    }
    return result;
  }
};

/// The hash functor for enabling use of *GroupKey* as a key in unordered
/// map data structures with transparent lookup.
struct GroupKeyHash {
  auto operator()(GroupKey const& x) const noexcept -> size_t {
    auto hasher = xxh64{};
    for (auto const& value : x) {
      hash_append(hasher, make_view(value));
    }
    return hasher.finish();
  }

  auto operator()(GroupKeyView const& x) const noexcept -> size_t {
    auto hasher = xxh64{};
    for (auto const& value : x) {
      hash_append(hasher, value);
    }
    return hasher.finish();
  }
};

/// The equality functor for enabling use of *GroupKey* as a key in
/// unordered map data structures with transparent lookup.
struct GroupKeyEqual {
  using is_transparent = void;

  auto operator()(GroupKeyView const& x, GroupKey const& y) const noexcept
    -> bool {
    return std::equal(x.begin(), x.end(), y.begin(), y.end(),
                      [](auto const& lhs, auto const& rhs) {
                        return lhs == rhs;
                      });
  }

  auto operator()(GroupKey const& x, GroupKeyView const& y) const noexcept
    -> bool {
    return std::equal(x.begin(), x.end(), y.begin(), y.end(),
                      [](auto const& lhs, auto const& rhs) {
                        return lhs == rhs;
                      });
  }

  auto operator()(GroupKey const& x, GroupKey const& y) const noexcept -> bool {
    return x == y;
  }

  auto operator()(GroupKeyView const& x, GroupKeyView const& y) const noexcept
    -> bool {
    return x == y;
  }
};

struct Aggregate {
  Option<ast::field_path> dest;
  ast::function_call call;

  friend auto inspect(auto& f, Aggregate& x) -> bool {
    return f.object(x).fields(f.field("dest", x.dest), f.field("call", x.call));
  }
};

struct Group {
  Option<ast::field_path> dest;
  ast::field_path expr;

  friend auto inspect(auto& f, Group& x) -> bool {
    return f.object(x).fields(f.field("dest", x.dest), f.field("expr", x.expr));
  }
};

TENZIR_ENUM(Emission, final, event, timer);
TENZIR_ENUM(Mode, reset, cumulative);
TENZIR_ENUM(Output, summary, trigger, events);

struct Config {
  std::vector<Aggregate> aggregates;
  std::vector<Group> groups;

  /// Because we allow mixing aggregates and groups and want to emit them in the
  /// same order, we need to store some additional information, unless we use
  /// something like `vector<variant<Aggregate, ast::selector>>` instead. But
  /// that makes it more tricky to `zip`. If the index is positive, it
  /// corresponds to `aggregates`, otherwise `groups[-index - 1]`.
  std::vector<int64_t> indices;

  /// Unevaluated expression for the `emit` option. Evaluated after let
  /// substitution by evaluate_options().
  Option<ast::expression> emit_expr;

  /// Unevaluated expression for the `mode` option. Evaluated after let
  /// substitution by evaluate_options().
  Option<ast::expression> mode_expr;

  /// Unevaluated expression for the `output` option. Evaluated after let
  /// substitution by evaluate_options().
  Option<ast::expression> output_expr;

  /// The legacy `frequency` option, retained only for migration diagnostics.
  Option<ast::expression> legacy_frequency_expr;

  Emission emission = Emission::final;
  Mode mode = Mode::reset;
  Output output = Output::summary;
  int64_t emit_every = 1;
  Option<duration> emit_interval;

  friend auto inspect(auto& f, Config& x) -> bool {
    return f.object(x).fields(
      f.field("aggregates", x.aggregates), f.field("groups", x.groups),
      f.field("indices", x.indices), f.field("emit_expr", x.emit_expr),
      f.field("mode_expr", x.mode_expr), f.field("output_expr", x.output_expr),
      f.field("legacy_frequency_expr", x.legacy_frequency_expr),
      f.field("emission", x.emission), f.field("mode", x.mode),
      f.field("output", x.output), f.field("emit_every", x.emit_every),
      f.field("emit_interval", x.emit_interval));
  }
};

template <class Value>
using GroupMap = tsl::robin_map<GroupKey, Value, GroupKeyHash, GroupKeyEqual>;

struct Bucket {
  std::vector<Box<aggregation_instance>> aggregations;
  /// Derived values for final event replay. These are populated only after the
  /// input ends and are intentionally excluded from checkpoints.
  Option<std::vector<data>> final_values;
};

/// The largest number of distinct groups per input slice that we keep sizing
/// the partitioning allocations for. A single unusually diverse slice would
/// otherwise make every later slice reserve for a peak it never reaches again,
/// and growing the containers on demand costs less than that.
constexpr auto max_group_hint = size_t{4096};

/// The rows of a single input slice that belong to one group.
struct SlicePartition {
  GroupKey key;
  table_slice rows;
};

/// Partitions `slice` into one part per distinct group key, where the key of a
/// row is the tuple of `group_values` at that row.
///
/// Parts are returned in first-seen key order, rows keep their relative order
/// within a part, and every row appears in exactly one part. Both properties
/// are load-bearing: the former determines the order in which groups are
/// emitted, and the latter is what order-sensitive aggregations such as
/// `first`, `last`, and `collect` observe.
///
/// Inputs that are already clustered by key are partitioned into zero-copy
/// sub-slices. Otherwise the rows are permuted with a single `Take` over the
/// whole batch, after which every part again is a contiguous, zero-copy
/// sub-slice of that permutation. This keeps the number of Arrow copies at one
/// per slice instead of one per group, which matters because interleaved keys
/// degenerate to one contiguous run per row.
///
/// `group_hint` is an estimate for the number of distinct keys, used only to
/// size the internal allocations.
///
/// Requires that every element of `group_values` has `slice.rows()` values.
auto partition_by_key(table_slice const& slice,
                      std::span<multi_series const> group_values,
                      size_t group_hint) -> std::vector<SlicePartition> {
  auto const total_rows = detail::narrow<int64_t>(slice.rows());
  if (total_rows == 0) {
    return {};
  }
  // Assign a dense group index to every row in a single pass. Indices are
  // handed out in first-seen order, so iterating them in order reproduces the
  // order in which the keys occur in the input.
  auto index_of = GroupMap<uint32_t>{};
  index_of.reserve(group_hint);
  auto keys = std::vector<GroupKey>{};
  keys.reserve(group_hint);
  auto row_groups = std::vector<uint32_t>{};
  row_groups.reserve(static_cast<size_t>(total_rows));
  auto key = GroupKeyView{};
  key.reserve(group_values.size());
  auto previous = uint32_t{0};
  auto have_previous = false;
  for (auto row = int64_t{0}; row < total_rows; ++row) {
    key.clear();
    for (auto const& values : group_values) {
      key.emplace_back(values.view3_at(row));
    }
    // Comparing against the previous row's key avoids hashing and probing for
    // clustered inputs; a hash lookup happens only at key transitions.
    if (have_previous and GroupKeyEqual{}(key, keys[previous])) {
      row_groups.push_back(previous);
      continue;
    }
    auto it = index_of.find(key);
    if (it == index_of.end()) {
      auto const index = detail::narrow<uint32_t>(keys.size());
      keys.push_back(materialize(key));
      it = index_of.emplace_hint(it, keys.back(), index);
    }
    previous = it->second;
    have_previous = true;
    row_groups.push_back(previous);
  }
  auto const num_groups = keys.size();
  auto result = std::vector<SlicePartition>{};
  result.reserve(num_groups);
  // Everything lands in one group: forward the input untouched.
  if (num_groups == 1) {
    result.push_back({std::move(keys.front()), slice});
    return result;
  }
  // Counting sort by group index. `offsets` ends up holding the exclusive
  // prefix sums, so group `g` occupies `[offsets[g], offsets[g + 1])` of the
  // permutation, and the sort being stable preserves the relative row order
  // within every group.
  auto offsets = std::vector<int64_t>(num_groups + 1, 0);
  for (auto group : row_groups) {
    ++offsets[group + 1];
  }
  std::partial_sum(offsets.begin(), offsets.end(), offsets.begin());
  auto cursors = offsets;
  auto permutation = std::vector<int64_t>(static_cast<size_t>(total_rows));
  auto clustered = true;
  for (auto row = int64_t{0}; row < total_rows; ++row) {
    auto const target = cursors[row_groups[static_cast<size_t>(row)]]++;
    clustered = clustered and target == row;
    permutation[static_cast<size_t>(target)] = row;
  }
  // If the permutation is the identity, then the input is already clustered by
  // key and we can sub-slice it directly instead of copying it.
  auto const source = clustered ? slice : take_rows(slice, permutation);
  for (auto group = size_t{0}; group < num_groups; ++group) {
    result.push_back({
      std::move(keys[group]),
      subslice(source, detail::narrow<size_t>(offsets[group]),
               detail::narrow<size_t>(offsets[group + 1])),
    });
  }
  return result;
}

/// Returns the generated field name for an unnamed aggregate.
auto aggregate_name(Aggregate const& aggregate) -> std::string {
  auto const& call = aggregate.call;
  auto arg = std::invoke([&]() -> std::string {
    if (call.args.empty()) {
      return "";
    }
    if (call.args.size() > 1) {
      return "...";
    }
    auto sel = ast::field_path::try_from(call.args[0]);
    if (not sel) {
      return "...";
    }
    auto result = std::string{};
    if (sel->has_this()) {
      result = "this";
    }
    for (auto const& segment : sel->path()) {
      // TODO: This is wrong if the path contains special characters.
      if (not result.empty()) {
        result += '.';
      }
      result += segment.id.name;
    }
    return result;
  });
  return fmt::format("{}({})", call.fn.path[0].name, arg);
}

/// Returns the assignment destination of an aggregate, generating the same
/// name as final summarize output when no explicit destination was provided.
auto aggregate_destination(Aggregate const& aggregate) -> ast::field_path {
  if (aggregate.dest) {
    return *aggregate.dest;
  }
  auto result = ast::field_path::try_from(ast::root_field{
    ast::identifier{aggregate_name(aggregate), aggregate.call.get_location()}});
  TENZIR_ASSERT(result);
  return std::move(*result);
}

class AggregationState {
public:
  explicit AggregationState(Config config) : config_{std::move(config)} {
  }

  auto make_bucket(session ctx) -> Bucket {
    auto bucket = Bucket{};
    for (auto const& aggr : config_.aggregates) {
      // We already checked the cast and instantiation before.
      auto const* fn
        = dynamic_cast<aggregation_plugin const*>(&ctx.reg().get(aggr.call));
      TENZIR_ASSERT(fn);
      bucket.aggregations.push_back(Box<aggregation_instance>::from_non_null(
        fn->make_aggregation(function_invocation{aggr.call}, ctx).unwrap()));
    }
    return bucket;
  }

  auto add(table_slice const& slice, session ctx) -> void {
    saw_input_ = true;
    auto group_values = std::vector<multi_series>{};
    group_values.reserve(config_.groups.size());
    for (auto& group : config_.groups) {
      group_values.push_back(eval(group.expr.inner(), slice, ctx));
    }
    // Partition the slice by group first, then update each group's
    // aggregations exactly once. Aggregation instances evaluate their argument
    // expression on every update() call, so the number of updates must be
    // proportional to the number of *distinct groups* per slice rather than to
    // the number of group-key transitions. The latter degenerates to one
    // transition per row for interleaved inputs.
    auto partitions = partition_by_key(slice, group_values, max_slice_groups_);
    max_slice_groups_ = std::min(std::max(max_slice_groups_, partitions.size()),
                                 max_group_hint);
    // Groups are created in first-seen order, which determines the order in
    // which they are emitted.
    for (auto& partition : partitions) {
      auto it = groups_.find(partition.key);
      if (it == groups_.end()) {
        it = groups_.emplace_hint(it, std::move(partition.key),
                                  make_bucket(ctx));
      }
      for (auto& aggr : it.value().aggregations) {
        aggr->update(partition.rows, ctx);
      }
    }
  }

  auto add_events(table_slice const& slice, session ctx)
    -> std::vector<table_slice> {
    saw_input_ = true;
    if (config_.aggregates.empty()) {
      return {slice};
    }
    auto builders = std::vector<series_builder>{};
    builders.reserve(config_.aggregates.size());
    for (auto i = size_t{0}; i < config_.aggregates.size(); ++i) {
      builders.emplace_back();
    }
    auto const total_rows = detail::narrow<int64_t>(slice.rows());
    if (config_.mode == Mode::reset) {
      // Per-event reset never carries state across rows, so group keys are
      // irrelevant: reuse one scratch bucket and reset its aggregations after
      // every row instead of creating and destroying per-key buckets.
      if (not scratch_bucket_) {
        scratch_bucket_ = make_bucket(ctx);
      }
      for (auto row = int64_t{0}; row < total_rows; ++row) {
        auto input = subslice(slice, row, row + 1);
        for (auto [aggregation, builder] :
             std::views::zip(scratch_bucket_->aggregations, builders)) {
          aggregation->update(input, ctx);
          builder.data(aggregation->get());
          aggregation->reset();
        }
      }
    } else {
      TENZIR_ASSERT(config_.mode == Mode::cumulative);
      auto group_values = std::vector<multi_series>{};
      group_values.reserve(config_.groups.size());
      for (auto& group : config_.groups) {
        group_values.push_back(eval(group.expr.inner(), slice, ctx));
      }
      auto key = GroupKeyView{};
      key.reserve(config_.groups.size());
      for (auto row = int64_t{0}; row < total_rows; ++row) {
        key.clear();
        for (auto&& group : group_values) {
          key.emplace_back(group.view3_at(row));
        }
        auto it = groups_.find(key);
        if (it == groups_.end()) {
          it = groups_.emplace_hint(it, materialize(key), make_bucket(ctx));
        }
        auto input = subslice(slice, row, row + 1);
        for (auto [aggregation, builder] :
             std::views::zip(it.value().aggregations, builders)) {
          aggregation->update(input, ctx);
          builder.data(aggregation->get());
        }
      }
    }
    auto values = std::vector<multi_series>{};
    values.reserve(builders.size());
    for (auto& builder : builders) {
      values.emplace_back(builder.finish());
    }
    auto result = std::vector<table_slice>{};
    auto begin = int64_t{0};
    for (auto parts : split_multi_series(values)) {
      TENZIR_ASSERT(not parts.empty());
      auto end = begin + parts.front().length();
      auto output = subslice(slice, begin, end);
      begin = end;
      TENZIR_ASSERT(parts.size() == config_.aggregates.size());
      for (auto i = size_t{0}; i < config_.aggregates.size(); ++i) {
        output = assign(aggregate_destination(config_.aggregates[i]),
                        std::move(parts[i]), output, ctx);
      }
      result.push_back(std::move(output));
    }
    return result;
  }

  auto cache_final_values() -> void {
    TENZIR_ASSERT(config_.output == Output::events);
    for (auto it = groups_.begin(); it != groups_.end(); ++it) {
      auto& bucket = it.value();
      TENZIR_ASSERT(not bucket.final_values);
      auto values = std::vector<data>{};
      values.reserve(bucket.aggregations.size());
      for (auto const& aggregation : bucket.aggregations) {
        values.push_back(aggregation->get());
      }
      bucket.final_values = std::move(values);
    }
  }

  auto enrich(table_slice const& events, session ctx)
    -> std::vector<table_slice> {
    if (config_.aggregates.empty()) {
      return {events};
    }
    auto group_values = std::vector<multi_series>{};
    group_values.reserve(config_.groups.size());
    for (auto& group : config_.groups) {
      group_values.push_back(eval(group.expr.inner(), events, ctx));
    }
    auto builders = std::vector<series_builder>{};
    builders.reserve(config_.aggregates.size());
    for (auto i = size_t{0}; i < config_.aggregates.size(); ++i) {
      builders.emplace_back();
    }
    auto key = GroupKeyView{};
    key.reserve(config_.groups.size());
    auto const total_rows = detail::narrow<int64_t>(events.rows());
    for (auto row = int64_t{0}; row < total_rows; ++row) {
      key.clear();
      for (auto&& group : group_values) {
        key.emplace_back(group.view3_at(row));
      }
      auto it = groups_.find(key);
      TENZIR_ASSERT(it != groups_.end());
      if (config_.output == Output::events) {
        TENZIR_ASSERT(it.value().final_values);
        TENZIR_ASSERT(it.value().final_values->size() == builders.size());
        for (auto [value, builder] :
             std::views::zip(*it.value().final_values, builders)) {
          builder.data(value);
        }
      } else {
        for (auto [aggregation, builder] :
             std::views::zip(it.value().aggregations, builders)) {
          builder.data(aggregation->get());
        }
      }
    }
    auto values = std::vector<multi_series>{};
    values.reserve(builders.size());
    for (auto& builder : builders) {
      values.emplace_back(builder.finish());
    }
    auto result = std::vector<table_slice>{};
    auto begin = int64_t{0};
    for (auto parts : split_multi_series(values)) {
      TENZIR_ASSERT(not parts.empty());
      auto end = begin + parts.front().length();
      auto output = subslice(events, begin, end);
      begin = end;
      TENZIR_ASSERT(parts.size() == config_.aggregates.size());
      for (auto i = size_t{0}; i < config_.aggregates.size(); ++i) {
        output = assign(aggregate_destination(config_.aggregates[i]),
                        std::move(parts[i]), output, ctx);
      }
      result.push_back(std::move(output));
    }
    return result;
  }

  auto reset() -> void {
    groups_.clear();
    saw_input_ = false;
  }

  auto flush(session ctx) -> std::vector<table_slice> {
    return flush(false, ctx);
  }

  auto flush(bool force, session ctx) -> std::vector<table_slice> {
    // Avoid emitting before any input arrived unless explicitly forced (used
    // for final emission).
    if (not force and not saw_input_) {
      return {};
    }
    if (config_.mode == Mode::reset) {
      auto result = finish_impl(ctx);
      groups_.clear();
      saw_input_ = false;
      return result;
    }
    TENZIR_ASSERT(config_.mode == Mode::cumulative);
    return finish_impl(ctx);
  }

  auto finish(session ctx) -> std::vector<table_slice> {
    return finish_impl(ctx);
  }

  auto config() const -> Config const& {
    return config_;
  }

  auto saw_input() const noexcept -> bool {
    return saw_input_;
  }

private:
  auto finish_impl(session ctx) -> std::vector<table_slice> {
    // Special case: if there are no configured groups, and no groups were
    // created because we didn't get any input events, then we create a new
    // bucket and just finish it. That way, `from [] | summarize count()` will
    // return a single event showing a count of zero.
    if (config_.groups.empty() and groups_.empty()) {
      auto b = series_builder{};
      b.data(finish_group(GroupKey{}, make_bucket(ctx)));
      return b.finish_as_table_slice();
    }
    // TODO: Group by schema again to make this more efficient.
    auto b = series_builder{};
    for (auto const& [key, group] : groups_) {
      b.data(finish_group(key, group));
    }
    return b.finish_as_table_slice();
  }

  /// Writes `value` into `root` at the path described by `sel`.
  static auto
  emplace_value(record& root, ast::field_path const& sel, data value) -> void {
    if (sel.path().empty()) {
      // An empty path means the selector refers to `this` (the whole record).
      // Merge the value into root if it is a record; non-record values are
      // silently ignored because there is no meaningful field to assign them to.
      if (auto* rec = try_as<record>(&value)) {
        root = std::move(*rec);
      }
      return;
    }
    auto* current = &root;
    for (auto const& segment : sel.path()) {
      auto& val = (*current)[segment.id.name];
      if (&segment == &sel.path().back()) {
        val = std::move(value);
      } else {
        current = try_as<record>(&val);
        if (not current) {
          val = record{};
          current = &as<record>(val);
        }
      }
    }
  }

  /// Builds the output record for one group bucket.
  auto finish_group(GroupKey const& key, Bucket const& bucket) const -> record {
    auto result = record{};
    for (auto index : config_.indices) {
      if (index >= 0) {
        auto const& dest = config_.aggregates[index].dest;
        auto value = bucket.aggregations[index]->get();
        if (dest) {
          emplace_value(result, *dest, value);
        } else {
          result.emplace(aggregate_name(config_.aggregates[index]), value);
        }
      } else {
        auto group_index = -index - 1;
        auto const& group_def = config_.groups[group_index];
        auto const& dest = group_def.dest ? *group_def.dest : group_def.expr;
        emplace_value(result, dest, key[group_index]);
      }
    }
    return result;
  }

  // Aggregation instances serialize as one opaque blob per aggregate, so both
  // directions exchange a map from group key to aggregation blobs. Loading
  // rebuilds each bucket by restoring freshly instantiated aggregation
  // instances from the blobs. Snapshots are produced by the same operator
  // configuration that loads them, so restoration cannot fail.
  friend auto inspect(auto& f, AggregationState& x) -> bool {
    if constexpr (std::remove_reference_t<decltype(f)>::is_loading) {
      auto groups = GroupMap<std::vector<chunk_ptr>>{};
      auto on_load = [&] {
        // No diagnostics can occur here: the pipeline was already validated at
        // compile time.
        auto dh = null_diagnostic_handler{};
        auto provider = session_provider::make(dh);
        x.groups_.clear();
        for (auto it = groups.begin(); it != groups.end(); ++it) {
          auto bucket = x.make_bucket(provider.as_session());
          TENZIR_ASSERT(it.value().size() == bucket.aggregations.size());
          for (auto&& [aggregation, blob] :
               std::views::zip(bucket.aggregations, it.value())) {
            auto restored = aggregation->restore(std::move(blob));
            TENZIR_ASSERT(restored);
          }
          x.groups_.emplace(it->first, std::move(bucket));
        }
        return true;
      };
      return f.object(x).on_load(on_load).fields(
        f.field("saw_input", x.saw_input_), f.field("groups", groups));
    } else {
      auto groups = GroupMap<std::vector<chunk_ptr>>{};
      for (auto const& [key, bucket] : x.groups_) {
        auto blobs = std::vector<chunk_ptr>{};
        blobs.reserve(bucket.aggregations.size());
        for (auto const& aggregation : bucket.aggregations) {
          blobs.push_back(aggregation->save());
        }
        groups.emplace(key, std::move(blobs));
      }
      return f.object(x).fields(f.field("saw_input", x.saw_input_),
                                f.field("groups", groups));
    }
  }

  Config config_;
  GroupMap<Bucket> groups_;
  /// Scratch bucket for reset-mode event emission. Its aggregations are reset
  /// after every row, so it never carries state across calls and is
  /// intentionally not part of snapshots.
  Option<Bucket> scratch_bucket_;
  /// The largest number of distinct groups seen in a single input slice,
  /// capped at `max_group_hint` and used to size the per-slice partitioning
  /// allocations. Purely an optimization, hence not part of snapshots.
  size_t max_slice_groups_ = 0;
  bool saw_input_ = false;
};

// ---------------------------------------------------------------------------
// Configuration parsing
// ---------------------------------------------------------------------------

/// Classifies invocation arguments into a fully populated configuration.
auto build_config(std::vector<ast::expression> exprs, session ctx)
  -> failure_or<Config> {
  auto config = Config{};
  auto failed = false;
  // Keep option expressions unevaluated until let bindings are substituted.
  auto parse_options = [&](ast::record const& rec) {
    for (auto const& item : rec.items) {
      auto const* field = try_as<ast::record::field>(item);
      if (not field) {
        diagnostic::error("spread not allowed in options record")
          .primary(rec.get_location())
          .emit(ctx);
        failed = true;
        return;
      }
      auto const& name = field->name.name;
      if (name == "emit") {
        config.emit_expr = field->expr;
      } else if (name == "mode") {
        config.mode_expr = field->expr;
      } else if (name == "output") {
        config.output_expr = field->expr;
      } else if (name == "frequency") {
        config.legacy_frequency_expr = field->expr;
      } else {
        diagnostic::error("unknown option `{}`", name)
          .primary(field->name)
          .emit(ctx);
        failed = true;
        return;
      }
    }
  };
  auto add_aggregate
    = [&](Option<ast::field_path> dest, ast::function_call call) {
        auto const& fn = ctx.reg().get(call);
        if (not dynamic_cast<aggregation_plugin const*>(&fn)
            and not dynamic_cast<nova::AggregationPlugin const*>(&fn)) {
          diagnostic::error("function does not support aggregations")
            .primary(call.fn)
            .hint("if you want to group by this, use assignment before")
            .docs("https://tenzir.com/docs/operators/summarize")
            .emit(ctx);
          failed = true;
          return;
        }
        // Argument validation via make_aggregation is intentionally deferred:
        // args may contain unresolved let-bindings when called from compile().
        // SummarizeIr validates them after substituting let bindings.
        auto index = detail::narrow<int64_t>(config.aggregates.size());
        config.indices.push_back(index);
        config.aggregates.emplace_back(std::move(dest), std::move(call));
      };
  auto add_group = [&](Option<ast::field_path> dest, ast::field_path expr) {
    auto index = -detail::narrow<int64_t>(config.groups.size()) - 1;
    config.indices.push_back(index);
    config.groups.emplace_back(std::move(dest), std::move(expr));
  };
  for (auto& arg : exprs) {
    arg.match(
      [&](ast::function_call& arg) {
        add_aggregate(None{}, std::move(arg));
      },
      [&](ast::assignment& arg) {
        auto selector = ast::selector::try_from(arg.left);
        auto* left = selector ? try_as<ast::field_path>(&*selector) : nullptr;
        if (not left) {
          diagnostic::error("expected data selector, not meta")
            .primary(arg.left)
            .emit(ctx);
          failed = true;
          return;
        }
        // Check for `options=...` named argument
        if (not left->has_this() and left->path().size() == 1
            and left->path()[0].id.name == "options") {
          auto* rec = try_as<ast::record>(arg.right);
          if (not rec) {
            diagnostic::error("expected record for `options`")
              .primary(arg.right)
              .emit(ctx);
            failed = true;
            return;
          }
          parse_options(*rec);
          return;
        }
        arg.right.match(
          [&](ast::function_call& right) {
            add_aggregate(std::move(*left), std::move(right));
          },
          [&](auto&) {
            auto right = ast::field_path::try_from(arg.right);
            if (right) {
              add_group(std::move(*left), std::move(*right));
            } else {
              diagnostic::error(
                "expected selector or aggregation function call")
                .primary(arg.right)
                .emit(ctx);
              failed = true;
            }
          });
      },
      [&](auto&) {
        auto selector = ast::field_path::try_from(arg);
        if (selector) {
          add_group(None{}, std::move(*selector));
        } else {
          diagnostic::error(
            "expected selector, assignment or aggregation function call")
            .primary(arg)
            .emit(ctx);
          failed = true;
        }
      });
  }
  if (failed) {
    return failure::promise();
  }
  return config;
}

/// The smallest accepted timer interval. The timer catch-up loop in
/// `flush_until()` iterates once per elapsed interval, so impractically small
/// intervals would turn routine scheduling delay into unbounded work and, in
/// cumulative mode, one emitted summary per missed tick.
constexpr auto min_emit_interval = duration{std::chrono::milliseconds{10}};

/// Resolves the emission boundary, aggregation state, and output policy after
/// let substitution.
auto evaluate_options(Config& config, session ctx) -> failure_or<void> {
  if (config.emit_expr and config.legacy_frequency_expr) {
    diagnostic::error("`emit` and legacy `frequency` cannot be combined")
      .primary(*config.emit_expr)
      .secondary(*config.legacy_frequency_expr)
      .emit(ctx);
    return failure::promise();
  }
  if (config.mode_expr) {
    TRY(auto value, const_eval(*config.mode_expr, ctx));
    auto* str = try_as<std::string>(value.inner);
    if (not str) {
      diagnostic::error("expected string for `mode`")
        .primary(*config.mode_expr)
        .emit(ctx);
      return failure::promise();
    }
    if (*str == "reset") {
      config.mode = Mode::reset;
    } else if (*str == "cumulative") {
      config.mode = Mode::cumulative;
    } else if (*str == "update") {
      diagnostic::error("mode `update` is no longer supported")
        .primary(*config.mode_expr)
        .hint("use `group` followed by `deduplicate` to emit changed groups")
        .emit(ctx);
      return failure::promise();
    } else {
      diagnostic::error("invalid mode `{}`", *str)
        .primary(*config.mode_expr)
        .hint("expected `reset` or `cumulative`")
        .emit(ctx);
      return failure::promise();
    }
  }
  if (config.output_expr) {
    TRY(auto value, const_eval(*config.output_expr, ctx));
    auto* str = try_as<std::string>(value.inner);
    if (not str) {
      diagnostic::error("expected string for `output`")
        .primary(*config.output_expr)
        .emit(ctx);
      return failure::promise();
    }
    if (*str == "summary") {
      config.output = Output::summary;
    } else if (*str == "trigger") {
      config.output = Output::trigger;
    } else if (*str == "events") {
      config.output = Output::events;
    } else {
      diagnostic::error("invalid output `{}`", *str)
        .primary(*config.output_expr)
        .hint("expected `summary`, `trigger`, or `events`")
        .emit(ctx);
      return failure::promise();
    }
  }
  auto finish = [&]() -> failure_or<void> {
    if (not config.output_expr) {
      config.output = config.emission == Emission::event ? Output::trigger
                                                         : Output::summary;
    }
    if (config.output == Output::events
        and config.emission != Emission::final) {
      auto const& cadence
        = config.emit_expr ? *config.emit_expr : *config.legacy_frequency_expr;
      diagnostic::error("output `events` currently requires final emission")
        .primary(cadence)
        .secondary(*config.output_expr)
        .hint("remove the `emit` or `frequency` option")
        .emit(ctx);
      return failure::promise();
    }
    if (config.output_expr and config.output == Output::summary
        and config.emission == Emission::event) {
      diagnostic::error(
        "output `summary` is not yet supported with count-based emission")
        .primary(*config.output_expr)
        .secondary(*config.emit_expr)
        .emit(ctx);
      return failure::promise();
    }
    if (config.output_expr and config.output == Output::trigger
        and config.emission != Emission::event) {
      diagnostic::error("output `trigger` requires count-based emission")
        .primary(*config.output_expr)
        .emit(ctx);
      return failure::promise();
    }
    if (config.emit_expr and config.emission != Emission::final
        and not config.mode_expr) {
      diagnostic::error("`emit` requires `mode` to be set")
        .primary(*config.emit_expr)
        .emit(ctx);
      return failure::promise();
    }
    return {};
  };
  if (config.legacy_frequency_expr) {
    TRY(auto value, const_eval(*config.legacy_frequency_expr, ctx));
    auto* interval = try_as<duration>(value.inner);
    if (not interval) {
      diagnostic::error("expected duration for `frequency`")
        .primary(*config.legacy_frequency_expr)
        .emit(ctx);
      return failure::promise();
    }
    if (*interval < min_emit_interval) {
      diagnostic::error("`frequency` must be at least 10ms")
        .primary(*config.legacy_frequency_expr)
        .emit(ctx);
      return failure::promise();
    }
    diagnostic::warning("`frequency` is deprecated")
      .primary(*config.legacy_frequency_expr)
      .hint("replace `frequency` with `emit` and set `mode: \"{}\"`",
            config.mode)
      .emit(ctx);
    config.emission = Emission::timer;
    config.emit_interval = *interval;
    return finish();
  }
  if (config.emit_expr) {
    TRY(auto value, const_eval(*config.emit_expr, ctx));
    if (auto* count = try_as<int64_t>(value.inner)) {
      if (*count <= 0) {
        diagnostic::error("`emit` event count must be positive")
          .primary(*config.emit_expr)
          .emit(ctx);
        return failure::promise();
      }
      config.emission = Emission::event;
      config.emit_every = *count;
      return finish();
    }
    if (auto* interval = try_as<duration>(value.inner)) {
      if (*interval < min_emit_interval) {
        diagnostic::error("`emit` duration must be at least 10ms")
          .primary(*config.emit_expr)
          .emit(ctx);
        return failure::promise();
      }
      config.emission = Emission::timer;
      config.emit_interval = *interval;
      return finish();
    }
    if (auto* boundary = try_as<std::string>(value.inner);
        boundary and *boundary == "final") {
      config.emission = Emission::final;
      return finish();
    }
    diagnostic::error("expected int, duration, or `final` for `emit`")
      .primary(*config.emit_expr)
      .emit(ctx);
    return failure::promise();
  }
  // Preserve the existing shorthand: setting only `mode` means count-based
  // trigger output with a boundary of one event. An explicit output policy
  // keeps the default final boundary.
  if (config.mode_expr and not config.output_expr) {
    config.emission = Emission::event;
  }
  return finish();
}

class Summarize final : public Operator<table_slice, table_slice> {
public:
  explicit Summarize(Config config) : state_{std::move(config)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    // TODO: Support checkpointing for final event output once executor
    // snapshots can reference durable spill or sidecar storage.
    if (state_.config().output == Output::events
        and ctx.checkpoint_settings()) {
      checkpointing_rejected_ = true;
      TENZIR_ASSERT(state_.config().output_expr);
      diagnostic::error(
        "`summarize` with output `events` does not support checkpointing")
        .primary(*state_.config().output_expr)
        .note("final event output buffers input until the finite input ends")
        .emit(ctx);
      co_return;
    }
    provider_.emplace(session_provider::make(ctx.dh()));
    if (state_.config().emission == Emission::timer) {
      TENZIR_ASSERT(state_.config().emit_interval);
      auto emit_interval = *state_.config().emit_interval;
      ctx.spawn_task([emit_interval, frontier_queue = frontier_queue_,
                      tick_queue = tick_queue_]() mutable -> Task<void> {
        auto next_flush = co_await frontier_queue->dequeue();
        while (true) {
          while (auto frontier = frontier_queue->try_dequeue()) {
            next_flush = std::max(next_flush, *frontier);
          }
          co_await sleep_until(next_flush);
          co_await tick_queue->enqueue(TimerTick{next_flush});
          next_flush = detail::saturating_add(next_flush, emit_interval);
        }
      });
      // A restored snapshot carries the remaining time until the next flush;
      // re-arm the timer so that the cadence continues across a restart.
      if (auto remaining = std::exchange(restored_timer_remaining_, None{})) {
        arm_timer(*remaining);
      }
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return checkpointing_rejected_ ? OperatorState::done
                                   : OperatorState::normal;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (checkpointing_rejected_ or input.rows() == 0) {
      co_return;
    }
    if (state_.config().output == Output::events) {
      if (not state_.config().aggregates.empty()) {
        state_.add(input, provider_->as_session());
      }
      buffered_bytes_
        = detail::saturating_add(buffered_bytes_, input.approx_bytes());
      buffered_.push_back(std::move(input));
      warn_about_buffering(ctx);
      co_return;
    }
    if (state_.config().emission == Emission::event) {
      auto const emit_every = state_.config().emit_every;
      TENZIR_ASSERT(emit_every > 0);
      if (emit_every == 1) {
        for (auto& slice : state_.add_events(input, provider_->as_session())) {
          co_await push(std::move(slice));
        }
        co_return;
      }
      auto begin = int64_t{0};
      auto const rows = detail::narrow<int64_t>(input.rows());
      while (begin < rows) {
        auto const remaining = emit_every - events_since_emit_;
        auto const batch_size = std::min(remaining, rows - begin);
        auto const end = begin + batch_size;
        auto batch = subslice(input, begin, end);
        if (not state_.config().aggregates.empty()) {
          state_.add(batch, provider_->as_session());
        }
        events_since_emit_ += end - begin;
        pending_event_ = subslice(input, end - 1, end);
        if (events_since_emit_ == emit_every) {
          for (auto& slice :
               state_.enrich(pending_event_, provider_->as_session())) {
            co_await push(std::move(slice));
          }
          if (state_.config().mode == Mode::reset) {
            state_.reset();
          }
          events_since_emit_ = 0;
          pending_event_ = {};
        }
        begin = end;
      }
      co_return;
    }
    if (state_.config().emission == Emission::timer) {
      co_await flush_until(steady_clock::now(), push);
      if (not next_flush_) {
        arm_timer(*state_.config().emit_interval);
      }
    }
    state_.add(input, provider_->as_session());
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    if (checkpointing_rejected_) {
      co_return FinalizeBehavior::done;
    }
    if (state_.config().output == Output::events) {
      state_.cache_final_values();
      for (auto const& events : buffered_) {
        for (auto& slice : state_.enrich(events, provider_->as_session())) {
          co_await push(std::move(slice));
        }
      }
      co_return FinalizeBehavior::done;
    }
    if (state_.config().emission == Emission::event) {
      if (events_since_emit_ > 0) {
        TENZIR_ASSERT(pending_event_.rows() == 1);
        for (auto& slice :
             state_.enrich(pending_event_, provider_->as_session())) {
          co_await push(std::move(slice));
        }
      }
      co_return FinalizeBehavior::done;
    }
    // An empty input still produces the neutral aggregate result. Suppress
    // finalization only when an active reset timer already flushed all pending
    // rows, which avoids adding a synthetic empty interval at end-of-input.
    if (state_.config().emission == Emission::timer and not state_.saw_input()
        and next_flush_) {
      co_return FinalizeBehavior::done;
    }
    for (auto& slice : state_.finish(provider_->as_session())) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (state_.config().emission != Emission::timer) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await tick_queue_->dequeue();
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto* tick = result.try_as<TimerTick>();
    TENZIR_ASSERT(tick);
    co_await flush_until(tick->deadline, push);
  }

  auto snapshot(Serde& serde) -> void override {
    serde("state", state_);
    serde("events_since_emit", events_since_emit_);
    serde("pending_event", pending_event_);
    // Persist the timer as the remaining time until the next flush; absolute
    // steady_clock deadlines are meaningless across a restart.
    auto timer_remaining = Option<duration>{};
    if (next_flush_) {
      timer_remaining
        = std::max(*next_flush_ - steady_clock::now(), duration::zero());
    }
    serde("timer_remaining", timer_remaining);
    if (not next_flush_) {
      // Either we are loading, or we are saving before the timer was armed. In
      // the latter case `timer_remaining` is empty, so this is a no-op.
      restored_timer_remaining_ = timer_remaining;
    }
  }

private:
  struct TimerTick {
    steady_clock::time_point deadline;
  };

  // Carries the current flush frontier to the timer task. A single slot
  // suffices because only the latest frontier matters; `publish_frontier()`
  // coalesces a stale pending value instead of queueing behind it, so the
  // main operator path never blocks while the timer task waits for an older
  // tick to be consumed.
  using FrontierQueue = BoundedQueue<steady_clock::time_point>;
  using TickQueue = folly::coro::BoundedQueue<TimerTick>;

  auto publish_frontier(steady_clock::time_point frontier) -> void {
    // Drop a stale pending frontier, then publish the new one. Frontiers are
    // monotonically increasing on this side, and the timer task takes the
    // maximum over everything it dequeues, so racing with a concurrent
    // dequeue is harmless.
    while (not frontier_queue_->try_enqueue(frontier)) {
      std::ignore = frontier_queue_->try_dequeue();
    }
  }

  auto arm_timer(duration delay) -> void {
    TENZIR_ASSERT(not next_flush_);
    next_flush_ = detail::saturating_add(steady_clock::now(), delay);
    publish_frontier(*next_flush_);
  }

  auto warn_about_buffering(OpCtx& ctx) -> void {
    using namespace si_literals;
    static constexpr auto warning_threshold = uint64_t{512_Mi};
    if (warned_about_buffering_ or buffered_bytes_ < warning_threshold) {
      return;
    }
    diagnostic::warning("`summarize` buffered approximately {} MiB with "
                        "`output: \"events\"`",
                        buffered_bytes_ / 1_Mi)
      .note("event output starts only after the finite input ends")
      .hint("use `window` or reduce the input population to bound memory use")
      .emit(ctx);
    warned_about_buffering_ = true;
  }

  auto flush_until(steady_clock::time_point deadline, Push<table_slice>& push)
    -> Task<void> {
    if (not next_flush_) {
      co_return;
    }
    auto frontier_changed = false;
    TENZIR_ASSERT(state_.config().emit_interval);
    // This catch-up loop iterates once per elapsed interval, emitting one
    // summary per missed period in cumulative mode. `min_emit_interval`
    // bounds the work per unit of stalled time.
    while (*next_flush_ <= deadline) {
      frontier_changed = true;
      for (auto& slice : state_.flush(provider_->as_session())) {
        co_await push(std::move(slice));
      }
      *next_flush_
        = detail::saturating_add(*next_flush_, *state_.config().emit_interval);
    }
    if (frontier_changed) {
      publish_frontier(*next_flush_);
    }
  }

  AggregationState state_;
  bool checkpointing_rejected_ = false;
  int64_t events_since_emit_ = 0;
  table_slice pending_event_;
  std::vector<table_slice> buffered_;
  uint64_t buffered_bytes_ = 0;
  bool warned_about_buffering_ = false;
  Option<session_provider> provider_;
  Option<steady_clock::time_point> next_flush_;
  Option<duration> restored_timer_remaining_;
  Arc<FrontierQueue> frontier_queue_{std::in_place, 1};
  mutable Arc<TickQueue> tick_queue_{std::in_place, 1};
};

// ---------------------------------------------------------------------------
// Nova implementation
// ---------------------------------------------------------------------------

/// A materialized group key.
struct NovaGroupKey : std::vector<nova::Data> {
  using vector::vector;
};

/// A group key that borrows one row of the evaluated group columns.
struct NovaGroupKeyView : std::vector<nova::RowView<nova::Data>> {
  using vector::vector;
};

auto materialize(NovaGroupKeyView const& views) -> NovaGroupKey {
  auto builder = nova::ArrayBuilder<nova::Data>{};
  auto result = NovaGroupKey{};
  result.reserve(views.size());
  for (auto const& view : views) {
    nova::append_row(builder, view);
    result.push_back(builder.take_last());
  }
  return result;
}

template <class Key>
concept nova_group_key = concepts::one_of<Key, NovaGroupKey, NovaGroupKeyView>;

struct NovaGroupKeyHash {
  using is_transparent = void;

  /// Combines the value hashes like `nova::hash_rows`, so that hashes of
  /// whole key columns can be used for lookups.
  template <nova_group_key Key>
  auto operator()(Key const& key) const noexcept -> uint64_t {
    auto result = uint64_t{0};
    for (auto const& value : key) {
      result = tenzir::hash(result, static_cast<uint64_t>(nova::hash(
                                      nova::RowView<nova::Data>{value})));
    }
    return result;
  }
};

struct NovaGroupKeyEqual {
  using is_transparent = void;

  template <nova_group_key Lhs, nova_group_key Rhs>
  auto operator()(Lhs const& lhs, Rhs const& rhs) const -> bool {
    return std::ranges::equal(lhs, rhs, [](auto const& x, auto const& y) {
      return nova::equivalent(nova::RowView<nova::Data>{x},
                              nova::RowView<nova::Data>{y});
    });
  }
};

/// The aggregation states of one group.
struct NovaBucket {
  NovaGroupKey key;
  std::vector<Box<nova::AggregationState>> states;
  /// The aggregates for final event replay, populated once the input ended.
  Option<std::vector<nova::Data>> final_values;
};

/// Writes `value` into `root` at the path described by `sel`.
auto emplace_value(nova::Record& root, ast::field_path const& sel,
                   nova::Data value) -> void {
  if (sel.path().empty()) {
    // As in the legacy implementation, only a record can replace `this`.
    if (auto* record = std::get_if<nova::Record>(&value)) {
      root = std::move(*record);
    }
    return;
  }
  auto* current = &root;
  for (auto const& segment : sel.path()) {
    auto& field = (*current)[segment.id.name];
    if (&segment == &sel.path().back()) {
      field = std::move(value);
      return;
    }
    current = std::get_if<nova::Record>(&field);
    if (not current) {
      field = nova::Record{};
      current = std::get_if<nova::Record>(&field);
    }
  }
}

/// Assigns `column` to `dest` in the active rows of `events`. `source` is
/// where the aggregate was called, for diagnostics.
auto assign_column(ast::field_path const& dest, nova::Array<nova::Data> column,
                   location source, nova::Events events, diagnostic_handler& dh)
  -> nova::Events {
  auto value = nova::MaskedArray<nova::Array<nova::Data>>{std::move(column),
                                                          events.mask};
  if (dest.path().empty()) {
    // Like `set this = ...`: rows whose aggregate is not a record become
    // empty records.
    events.data
      = nova::records_or_empty(std::move(value), events.length(), source, dh);
    return events;
  }
  events.data = nova::assign_nested_field(std::move(events.data), dest.path(),
                                          std::move(value), dh);
  return events;
}

class NovaAggregationState {
public:
  NovaAggregationState(Config config, std::vector<nova::Evaluator> groups,
                       std::vector<Box<nova::Aggregation>> aggregations)
    : config_{std::move(config)},
      groups_{std::move(groups)},
      aggregations_{std::move(aggregations)} {
    TENZIR_ASSERT(groups_.size() == config_.groups.size());
    TENZIR_ASSERT(aggregations_.size() == config_.aggregates.size());
  }

  auto config() const -> Config const& {
    return config_;
  }

  auto saw_input() const noexcept -> bool {
    return saw_input_;
  }

  /// Folds the active rows of `events` into their groups.
  auto add(nova::Events const& events, diagnostic_handler& dh) -> void {
    saw_input_ = true;
    if (not events.mask.any()) {
      return;
    }
    auto ctx = nova::EvalCtx{dh};
    if (config_.groups.empty()) {
      auto& bucket = bucket_for(NovaGroupKeyView{});
      for (auto [aggregation, state] :
           std::views::zip(aggregations_, bucket.states)) {
        aggregation->update(events, *state, ctx);
      }
      return;
    }
    auto const columns = eval_groups(events, ctx);
    auto const active = static_cast<size_t>(events.mask.true_count());
    // Hash every key column once, instead of hashing each row's key through
    // the erased value during the lookup.
    auto hashes = std::vector<uint64_t>(active, 0);
    for (auto const& column : columns) {
      nova::hash_rows(column, events.mask, hashes);
    }
    // Assign every active row to its group, recording the groups in the order
    // in which the batch first mentions them. That order determines the order
    // in which new groups are emitted.
    auto touched = std::vector<size_t>{};
    auto row_slots = std::vector<size_t>{};
    row_slots.reserve(active);
    auto key = NovaGroupKeyView{};
    key.reserve(columns.size());
    auto previous = Option<size_t>{};
    auto previous_hash = uint64_t{0};
    auto i = size_t{0};
    for (auto row : nova::storage::true_bits(events.mask)) {
      auto const hash = hashes[i++];
      key.clear();
      for (auto const& column : columns) {
        key.push_back(column.get(row));
      }
      // Comparing against the previous row's key avoids the lookup for inputs
      // that are clustered by key.
      auto const index
        = previous and hash == previous_hash
              and NovaGroupKeyEqual{}(key, buckets_[*previous].key)
            ? *previous
            : bucket_index(key, hash);
      previous = index;
      previous_hash = hash;
      slots_.resize(buckets_.size(), no_slot);
      if (slots_[index] == no_slot) {
        slots_[index] = touched.size();
        touched.push_back(index);
      }
      row_slots.push_back(slots_[index]);
    }
    for (auto index : touched) {
      slots_[index] = no_slot;
    }
    // A batch that belongs to one group folds all of its active rows at once.
    if (touched.size() == 1) {
      auto& bucket = buckets_[touched.front()];
      for (auto [aggregation, state] :
           std::views::zip(aggregations_, bucket.states)) {
        aggregation->update(events, *state, ctx);
      }
      return;
    }
    // Sort the rows by group into one flat buffer, with a counting sort that
    // is stable: rows keep their relative order within a group, which
    // order-sensitive aggregations such as `first` and `collect` observe.
    auto offsets = std::vector<size_t>(touched.size() + 1, 0);
    for (auto slot : row_slots) {
      ++offsets[slot + 1];
    }
    std::partial_sum(offsets.begin(), offsets.end(), offsets.begin());
    auto sorted = std::vector<nova::storage::Index>(active);
    auto cursors = offsets;
    i = 0;
    for (auto row : nova::storage::true_bits(events.mask)) {
      sorted[cursors[row_slots[i++]]++] = row;
    }
    auto groups = std::vector<nova::AggregationGroup>{};
    groups.reserve(touched.size());
    for (auto a = size_t{0}; a < aggregations_.size(); ++a) {
      groups.clear();
      for (auto slot = size_t{0}; slot < touched.size(); ++slot) {
        groups.push_back({
          *buckets_[touched[slot]].states[a],
          std::span{sorted}.subspan(offsets[slot],
                                    offsets[slot + 1] - offsets[slot]),
        });
      }
      aggregations_[a]->update(events, groups, ctx);
    }
  }

  /// Emits one output row per active input row, carrying the aggregates of
  /// that row's group after folding it in.
  auto add_events(nova::Events const& events, diagnostic_handler& dh)
    -> nova::Events {
    saw_input_ = true;
    if (config_.aggregates.empty() or not events.mask.any()) {
      return events;
    }
    auto ctx = nova::EvalCtx{dh};
    auto builders
      = std::vector<nova::ArrayBuilder<nova::Data>>(config_.aggregates.size());
    auto const append
      = [&](nova::storage::Index row, NovaBucket& bucket, bool reset) {
          auto single = nova::subslice(events, row, row + 1);
          for (auto [aggregation, state, builder] :
               std::views::zip(aggregations_, bucket.states, builders)) {
            aggregation->update(single, *state, ctx);
            builder.skip_n(row - builder.length());
            nova::append_data(builder, state->get());
            if (reset) {
              state->reset();
            }
          }
        };
    if (config_.mode == Mode::reset) {
      // Per-event reset never carries state across rows, so group keys are
      // irrelevant: reuse one scratch bucket and reset it after every row.
      if (not scratch_) {
        scratch_ = make_bucket(NovaGroupKey{});
      }
      for (auto row : nova::storage::true_bits(events.mask)) {
        append(row, *scratch_, true);
      }
    } else {
      TENZIR_ASSERT(config_.mode == Mode::cumulative);
      auto const columns = eval_groups(events, ctx);
      auto key = NovaGroupKeyView{};
      key.reserve(columns.size());
      for (auto row : nova::storage::true_bits(events.mask)) {
        key.clear();
        for (auto const& column : columns) {
          key.push_back(column.get(row));
        }
        append(row, buckets_[bucket_index(key)], false);
      }
    }
    return assign_aggregates(events, std::move(builders), dh);
  }

  /// Adds the current aggregates of each active row's group to `events`.
  /// Every group must already exist.
  auto enrich(nova::Events const& events, diagnostic_handler& dh)
    -> nova::Events {
    if (config_.aggregates.empty() or not events.mask.any()) {
      return events;
    }
    auto ctx = nova::EvalCtx{dh};
    auto const columns = eval_groups(events, ctx);
    auto builders
      = std::vector<nova::ArrayBuilder<nova::Data>>(config_.aggregates.size());
    auto key = NovaGroupKeyView{};
    key.reserve(columns.size());
    for (auto row : nova::storage::true_bits(events.mask)) {
      key.clear();
      for (auto const& column : columns) {
        key.push_back(column.get(row));
      }
      auto it = index_.find(key);
      TENZIR_ASSERT(it != index_.end());
      auto const& bucket = buckets_[it->second];
      for (auto i = size_t{0}; i < builders.size(); ++i) {
        builders[i].skip_n(row - builders[i].length());
        if (config_.output == Output::events) {
          TENZIR_ASSERT(bucket.final_values);
          nova::append_data(builders[i], (*bucket.final_values)[i]);
        } else {
          nova::append_data(builders[i], bucket.states[i]->get());
        }
      }
    }
    return assign_aggregates(events, std::move(builders), dh);
  }

  auto cache_final_values() -> void {
    TENZIR_ASSERT(config_.output == Output::events);
    for (auto& bucket : buckets_) {
      TENZIR_ASSERT(not bucket.final_values);
      auto values = std::vector<nova::Data>{};
      values.reserve(bucket.states.size());
      for (auto const& state : bucket.states) {
        values.push_back(state->get());
      }
      bucket.final_values = std::move(values);
    }
  }

  auto reset() -> void {
    buckets_.clear();
    index_.clear();
    slots_.clear();
    saw_input_ = false;
  }

  auto flush() -> Option<nova::Events> {
    if (not saw_input_) {
      return None{};
    }
    auto result = finish();
    if (config_.mode == Mode::reset) {
      reset();
    }
    return result;
  }

  /// One output row per group, in first-seen order.
  auto finish() const -> Option<nova::Events> {
    auto builder = nova::ArrayBuilder<nova::Record>{};
    // Without groups and without input there is no bucket yet. A fresh one
    // makes `from [] | summarize count()` return a count of zero.
    if (config_.groups.empty() and buckets_.empty()) {
      append_group(builder, make_bucket(NovaGroupKey{}));
    }
    for (auto const& bucket : buckets_) {
      append_group(builder, bucket);
    }
    auto const length = builder.length();
    if (length == 0) {
      return None{};
    }
    return nova::Events{
      builder.finish(),
      nova::storage::BitMap{length, true},
      nova::Events::Meta::make_empty(length, "tenzir.summarize"),
    };
  }

private:
  static constexpr auto no_slot = std::numeric_limits<size_t>::max();

  auto eval_groups(nova::Events const& events, nova::EvalCtx ctx)
    -> std::vector<nova::Array<nova::Data>> {
    auto result = std::vector<nova::Array<nova::Data>>{};
    result.reserve(groups_.size());
    for (auto& group : groups_) {
      result.push_back(group.eval(events, ctx));
    }
    return result;
  }

  auto make_bucket(NovaGroupKey key) const -> NovaBucket {
    auto bucket = NovaBucket{std::move(key), {}, None{}};
    bucket.states.reserve(aggregations_.size());
    for (auto const& aggregation : aggregations_) {
      bucket.states.push_back(aggregation->make_state());
    }
    return bucket;
  }

  /// The index of the bucket for `key`, creating it if necessary.
  auto bucket_index(NovaGroupKeyView const& key) -> size_t {
    return bucket_index(key, NovaGroupKeyHash{}(key));
  }

  /// As above, for a key whose `NovaGroupKeyHash` is already known.
  auto bucket_index(NovaGroupKeyView const& key, uint64_t hash) -> size_t {
    auto it = index_.find(key, hash);
    if (it != index_.end()) {
      return it->second;
    }
    auto const index = buckets_.size();
    auto materialized = materialize(key);
    index_.emplace(materialized, index);
    buckets_.push_back(make_bucket(std::move(materialized)));
    return index;
  }

  auto bucket_for(NovaGroupKeyView const& key) -> NovaBucket& {
    return buckets_[bucket_index(key)];
  }

  auto assign_aggregates(nova::Events events,
                         std::vector<nova::ArrayBuilder<nova::Data>> builders,
                         diagnostic_handler& dh) const -> nova::Events {
    auto const length = events.length();
    for (auto [aggregate, builder] :
         std::views::zip(config_.aggregates, builders)) {
      builder.skip_n(length - builder.length());
      events
        = assign_column(aggregate_destination(aggregate), builder.finish(),
                        aggregate.call.get_location(), std::move(events), dh);
    }
    return events;
  }

  auto append_group(nova::ArrayBuilder<nova::Record>& builder,
                    NovaBucket const& bucket) const -> void {
    auto result = nova::Record{};
    for (auto index : config_.indices) {
      if (index >= 0) {
        auto const& aggregate = config_.aggregates[index];
        auto value = bucket.final_values ? (*bucket.final_values)[index]
                                         : bucket.states[index]->get();
        if (aggregate.dest) {
          emplace_value(result, *aggregate.dest, std::move(value));
        } else {
          result[aggregate_name(aggregate)] = std::move(value);
        }
      } else {
        auto const group_index = static_cast<size_t>(-index - 1);
        auto const& group = config_.groups[group_index];
        emplace_value(result, group.dest ? *group.dest : group.expr,
                      bucket.key[group_index]);
      }
    }
    auto record = builder.record();
    for (auto const& [name, value] : result) {
      nova::append_data(record.field(name), value);
    }
  }

  Config config_;
  /// The evaluators of the group-by keys, in the order of `config_.groups`.
  std::vector<nova::Evaluator> groups_;
  /// The prepared aggregations, in the order of `config_.aggregates`. Every
  /// bucket holds one state for each.
  std::vector<Box<nova::Aggregation>> aggregations_;
  /// The groups in first-seen order.
  std::vector<NovaBucket> buckets_;
  tsl::robin_map<NovaGroupKey, size_t, NovaGroupKeyHash, NovaGroupKeyEqual>
    index_;
  /// Per bucket, its slot among the groups of the batch being added, or
  /// `no_slot`. Kept across batches to avoid reallocating it.
  std::vector<size_t> slots_;
  /// The bucket for per-event emission in reset mode.
  Option<NovaBucket> scratch_;
  bool saw_input_ = false;
};

class SummarizeNova final : public Operator<nova::Events, nova::Events> {
public:
  SummarizeNova(Config config, location self)
    : config_{std::move(config)}, self_{self} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    // TODO: Support checkpointing once nova aggregation states can be
    // serialized.
    if (ctx.checkpoint_settings()) {
      done_ = true;
      diagnostic::error(
        "`summarize` does not support checkpointing with `--nova` yet")
        .primary(self_)
        .emit(ctx);
      co_return;
    }
    auto groups = std::vector<nova::Evaluator>{};
    groups.reserve(config_.groups.size());
    for (auto const& group : config_.groups) {
      auto evaluator = co_await nova::Evaluator::make(group.expr.inner(), ctx);
      if (not evaluator) {
        done_ = true;
        co_return;
      }
      groups.push_back(std::move(*evaluator));
    }
    auto aggregations = std::vector<Box<nova::Aggregation>>{};
    aggregations.reserve(config_.aggregates.size());
    for (auto const& aggregate : config_.aggregates) {
      auto aggregation = co_await nova::Aggregation::make(
        ast::expression{aggregate.call}, ctx);
      if (not aggregation) {
        done_ = true;
        co_return;
      }
      aggregations.push_back(std::move(*aggregation));
    }
    state_.emplace(config_, std::move(groups), std::move(aggregations));
    if (config_.emission == Emission::timer) {
      TENZIR_ASSERT(config_.emit_interval);
      auto emit_interval = *config_.emit_interval;
      ctx.spawn_task([emit_interval, frontier_queue = frontier_queue_,
                      tick_queue = tick_queue_]() mutable -> Task<void> {
        auto next_flush = co_await frontier_queue->dequeue();
        while (true) {
          while (auto frontier = frontier_queue->try_dequeue()) {
            next_flush = std::max(next_flush, *frontier);
          }
          co_await sleep_until(next_flush);
          co_await tick_queue->enqueue(TimerTick{next_flush});
          next_flush = detail::saturating_add(next_flush, emit_interval);
        }
      });
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_ or not input.mask.any()) {
      co_return;
    }
    if (config_.output == Output::events) {
      if (not config_.aggregates.empty()) {
        state_->add(input, ctx.dh());
      }
      buffered_bytes_ = detail::saturating_add(
        buffered_bytes_, static_cast<uint64_t>(input.approx_bytes()));
      buffered_.push_back(std::move(input));
      warn_about_buffering(ctx);
      co_return;
    }
    if (config_.emission == Emission::event) {
      auto const emit_every = config_.emit_every;
      TENZIR_ASSERT(emit_every > 0);
      if (emit_every == 1) {
        co_await push(state_->add_events(input, ctx.dh()));
        co_return;
      }
      // Batches are counted in active rows, which need not be contiguous.
      auto remaining = input.mask;
      while (remaining.any()) {
        auto batch = input;
        batch.mask = remaining.keep_first(emit_every - events_since_emit_);
        remaining = std::move(remaining).and_not(batch.mask);
        if (not config_.aggregates.empty()) {
          state_->add(batch, ctx.dh());
        }
        events_since_emit_ += batch.mask.true_count();
        auto last = nova::storage::Index{0};
        nova::storage::for_each_true(batch.mask, [&](auto row) {
          last = row;
        });
        pending_event_ = nova::subslice(input, last, last + 1);
        if (events_since_emit_ == emit_every) {
          co_await push(state_->enrich(*pending_event_, ctx.dh()));
          if (config_.mode == Mode::reset) {
            state_->reset();
          }
          events_since_emit_ = 0;
          pending_event_ = None{};
        }
      }
      co_return;
    }
    if (config_.emission == Emission::timer) {
      co_await flush_until(steady_clock::now(), push);
      if (not next_flush_) {
        arm_timer(*config_.emit_interval);
      }
    }
    state_->add(input, ctx.dh());
  }

  auto finalize(Push<nova::Events>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    if (done_) {
      co_return FinalizeBehavior::done;
    }
    if (config_.output == Output::events) {
      state_->cache_final_values();
      for (auto const& events : buffered_) {
        co_await push(state_->enrich(events, ctx.dh()));
      }
      co_return FinalizeBehavior::done;
    }
    if (config_.emission == Emission::event) {
      if (pending_event_) {
        co_await push(state_->enrich(*pending_event_, ctx.dh()));
      }
      co_return FinalizeBehavior::done;
    }
    // An empty input still produces the neutral aggregate result. Suppress
    // finalization only when an active reset timer already flushed all pending
    // rows, which avoids adding a synthetic empty interval at end-of-input.
    if (config_.emission == Emission::timer and not state_->saw_input()
        and next_flush_) {
      co_return FinalizeBehavior::done;
    }
    if (auto output = state_->finish()) {
      co_await push(std::move(*output));
    }
    co_return FinalizeBehavior::done;
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (config_.emission != Emission::timer) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    co_return co_await tick_queue_->dequeue();
  }

  auto process_task(Any result, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    auto* tick = result.try_as<TimerTick>();
    TENZIR_ASSERT(tick);
    co_await flush_until(tick->deadline, push);
  }

  auto snapshot(Serde&) -> void override {
    // `start` rejects checkpointing, so there is never a snapshot to take.
  }

private:
  struct TimerTick {
    steady_clock::time_point deadline;
  };

  using FrontierQueue = BoundedQueue<steady_clock::time_point>;
  using TickQueue = folly::coro::BoundedQueue<TimerTick>;

  auto publish_frontier(steady_clock::time_point frontier) -> void {
    // Drop a stale pending frontier, then publish the new one; see the legacy
    // implementation for why racing with the timer task is harmless.
    while (not frontier_queue_->try_enqueue(frontier)) {
      std::ignore = frontier_queue_->try_dequeue();
    }
  }

  auto arm_timer(duration delay) -> void {
    TENZIR_ASSERT(not next_flush_);
    next_flush_ = detail::saturating_add(steady_clock::now(), delay);
    publish_frontier(*next_flush_);
  }

  auto warn_about_buffering(OpCtx& ctx) -> void {
    using namespace si_literals;
    static constexpr auto warning_threshold = uint64_t{512_Mi};
    if (warned_about_buffering_ or buffered_bytes_ < warning_threshold) {
      return;
    }
    diagnostic::warning("`summarize` buffered approximately {} MiB with "
                        "`output: \"events\"`",
                        buffered_bytes_ / 1_Mi)
      .note("event output starts only after the finite input ends")
      .hint("use `window` or reduce the input population to bound memory use")
      .emit(ctx);
    warned_about_buffering_ = true;
  }

  auto flush_until(steady_clock::time_point deadline, Push<nova::Events>& push)
    -> Task<void> {
    if (not next_flush_) {
      co_return;
    }
    auto frontier_changed = false;
    TENZIR_ASSERT(config_.emit_interval);
    // One summary per elapsed interval; `min_emit_interval` bounds the work
    // per unit of stalled time.
    while (*next_flush_ <= deadline) {
      frontier_changed = true;
      if (auto output = state_->flush()) {
        co_await push(std::move(*output));
      }
      *next_flush_
        = detail::saturating_add(*next_flush_, *config_.emit_interval);
    }
    if (frontier_changed) {
      publish_frontier(*next_flush_);
    }
  }

  Config config_;
  location self_;
  Option<NovaAggregationState> state_;
  bool done_ = false;
  int64_t events_since_emit_ = 0;
  Option<nova::Events> pending_event_;
  std::vector<nova::Events> buffered_;
  uint64_t buffered_bytes_ = 0;
  bool warned_about_buffering_ = false;
  Option<steady_clock::time_point> next_flush_;
  Arc<FrontierQueue> frontier_queue_{std::in_place, 1};
  mutable Arc<TickQueue> tick_queue_{std::in_place, 1};
};

class SummarizeIr final : public ir::Operator {
public:
  SummarizeIr() = default;

  SummarizeIr(location self, Config config)
    : self_{self}, config_{std::move(config)} {
  }

  auto name() const -> std::string override {
    return "summarize";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    // Substitute through function-call arguments in aggregates; they can
    // reference let-bindings.  Group field-paths are static identifiers.
    for (auto& aggregate : config_.aggregates) {
      for (auto& arg : aggregate.call.args) {
        TRY(arg.substitute(ctx));
      }
    }
    // Substitute option expressions before evaluating the emission policy.
    if (config_.emit_expr) {
      TRY(config_.emit_expr->substitute(ctx));
    }
    if (config_.mode_expr) {
      TRY(config_.mode_expr->substitute(ctx));
    }
    if (config_.output_expr) {
      TRY(config_.output_expr->substitute(ctx));
    }
    if (config_.legacy_frequency_expr) {
      TRY(config_.legacy_frequency_expr->substitute(ctx));
    }
    // Constant options require fully-resolved let bindings. Aggregation
    // arguments are validated during planning, once the input is known.
    if (instantiate) {
      auto provider = session_provider::make(ctx);
      TRY(evaluate_options(config_, provider.as_session()));
    }
    return {};
  }

  auto spawn(element_type_tag input) const -> AnyOperator override {
    if (input.is<nova::Events>()) {
      return SummarizeNova{config_, self_}.with_name("summarize");
    }
    TENZIR_ASSERT(input.is<table_slice>());
    return Summarize{config_}.with_name("summarize");
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<nova::Events>() and input.is_not<table_slice>()) {
      diagnostic::error("operator expects events").primary(self_).emit(dh);
      return failure::promise();
    }
    auto const reg = global_registry();
    for (auto const& aggregate : config_.aggregates) {
      auto const& fn = reg->get(aggregate.call);
      if (input.is<nova::Events>()) {
        if (not dynamic_cast<nova::AggregationPlugin const*>(&fn)) {
          diagnostic::error("`{}` does not support `--nova` yet",
                            fn.function_name())
            .primary(aggregate.call)
            .emit(dh);
          return failure::promise();
        }
      } else if (not dynamic_cast<aggregation_plugin const*>(&fn)) {
        diagnostic::error("`{}` requires `--nova`", fn.function_name())
          .primary(aggregate.call)
          .emit(dh);
        return failure::promise();
      }
    }
    return input;
  }

  auto plan(ir::PlanBuilder& builder, ir::PlanPorts input,
            diagnostic_handler& dh) && -> failure_or<ir::PlanPorts> override {
    auto input_type
      = input.empty() ? element_type_tag{tag_v<void>} : input.front().type;
    TRY(infer_type(input_type, dh));
    // Type inference also probes uninstantiated pipelines. Planning happens
    // after substitution, so constant arguments can be validated here without
    // requiring lifecycle state in the operator.
    auto provider = session_provider::make(dh);
    auto ctx = provider.as_session();
    for (auto const& aggregate : config_.aggregates) {
      if (input_type.is<nova::Events>()) {
        TRY(nova::Aggregation::make(ast::expression{aggregate.call},
                                    nova::InstantiateCtx{ctx.dh(), ctx.reg()}));
      } else {
        auto const* legacy = dynamic_cast<aggregation_plugin const*>(
          &ctx.reg().get(aggregate.call));
        TENZIR_ASSERT(legacy);
        TRY(legacy->make_aggregation(function_invocation{aggregate.call}, ctx));
      }
    }
    return std::move(*this).ir::Operator::plan(builder, std::move(input), dh);
  }

  auto parallelizable() const -> bool override {
    // Without group-by keys, there is a single global aggregation state that
    // all rows must reach. Replicating the operator would yield one partial
    // result per instance.
    if (config_.groups.empty()) {
      return false;
    }
    // Counted event emission requires one instance to keep a pipeline-wide
    // event count.
    if (config_.emission == Emission::event and config_.emit_every != 1) {
      return false;
    }
    // Other final and per-event emission can use hash partitioning because it
    // routes each group to exactly one instance.
    return true;
  }

  auto partition_keys() const -> std::vector<ast::expression> override {
    // Rows with equal group-by keys must reach the same instance so that each
    // group's aggregation state lives in exactly one place. We key on the
    // input expressions rather than the output names.
    auto result = std::vector<ast::expression>{};
    result.reserve(config_.groups.size());
    for (auto const& group : config_.groups) {
      result.push_back(group.expr.inner());
    }
    return result;
  }

  auto main_location() const -> location override {
    return self_;
  }

  friend auto inspect(auto& f, SummarizeIr& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_),
                              f.field("cfg", x.config_));
  }

private:
  location self_;
  Config config_;
};

} // namespace

auto compile_summarize(ast::invocation inv, compile_ctx ctx)
  -> failure_or<ir::CompileResult> {
  // We use `operator_compiler_plugin` rather than
  // `OperatorPlugin`/`Describer` because `GenericIr` unconditionally routes
  // any `ast::assignment` arg to the named-argument path (look up LHS in a
  // fixed `desc->named` list, error if absent).  `summarize` uses assignments
  // *positionally*: the LHS is the output rename and the RHS determines the
  // kind (aggregate vs. group vs. options). Adding variadic named args to
  // `Describer` would still pre-split named from positional before
  // `build_config()` can see them together, requiring awkward reconstruction.
  // `compile()` receives the raw `inv.args` unchanged, so `build_config()`
  // can apply its own classification logic directly.
  auto loc = inv.op.get_location();
  // Bind all arguments except pipeline expressions before parsing.
  for (auto& arg : inv.args) {
    if (not is<ast::pipeline_expr>(arg)) {
      TRY(arg.bind(ctx));
    }
  }
  auto provider = session_provider::make(ctx);
  TRY(auto config, build_config(std::move(inv.args), provider.as_session()));
  return SummarizeIr{loc, std::move(config)};
}

class plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "summarize";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    return compile_summarize(std::move(inv), ctx);
  }
};

} // namespace tenzir::plugins::summarize

TENZIR_REGISTER_PLUGIN(tenzir::plugins::summarize::plugin)
TENZIR_REGISTER_PLUGIN(
  (tenzir::inspection_plugin<tenzir::ir::Operator,
                             tenzir::plugins::summarize::SummarizeIr>))
