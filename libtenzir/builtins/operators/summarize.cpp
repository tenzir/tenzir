//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arc.hpp>
#include <tenzir/arrow_memory_pool.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/bounded_queue.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/box.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/detail/saturating_arithmetic.hpp>
#include <tenzir/error.hpp>
#include <tenzir/hash/hash_append.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/multi_series.hpp>
#include <tenzir/option.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/session.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/registry.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/type.hpp>

#include <arrow/compute/api_scalar.h>
#include <arrow/compute/api_vector.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <folly/coro/BoundedQueue.h>
#include <tsl/robin_map.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <ranges>
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

  /// The legacy `frequency` option, retained only for migration diagnostics.
  Option<ast::expression> legacy_frequency_expr;

  Emission emission = Emission::final;
  Mode mode = Mode::reset;
  int64_t emit_every = 1;
  Option<duration> emit_interval;

  friend auto inspect(auto& f, Config& x) -> bool {
    return f.object(x).fields(
      f.field("aggregates", x.aggregates), f.field("groups", x.groups),
      f.field("indices", x.indices), f.field("emit_expr", x.emit_expr),
      f.field("mode_expr", x.mode_expr),
      f.field("legacy_frequency_expr", x.legacy_frequency_expr),
      f.field("emission", x.emission), f.field("mode", x.mode),
      f.field("emit_every", x.emit_every),
      f.field("emit_interval", x.emit_interval));
  }
};

template <class Value>
using GroupMap = tsl::robin_map<GroupKey, Value, GroupKeyHash, GroupKeyEqual>;

struct Bucket {
  std::vector<Box<aggregation_instance>> aggregations;
};

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
    for (auto& group : config_.groups) {
      group_values.push_back(eval(group.expr.inner(), slice, ctx));
    }
    auto key = GroupKeyView{};
    key.reserve(config_.groups.size());
    auto fill_key = [&](int64_t row) {
      key.clear();
      for (auto&& group : group_values) {
        key.emplace_back(group.view3_at(row));
      }
    };
    // Collect the row ranges of every group in this slice first, then update
    // each group's aggregations exactly once. Aggregation instances evaluate
    // their argument expression on every update() call, so the number of
    // updates must be proportional to the number of *distinct groups* per
    // slice rather than the number of group-key transitions. The latter
    // degenerates to one transition per row for interleaved inputs, which
    // makes per-transition updates prohibitively expensive.
    struct SliceGroup {
      GroupKey key;
      std::vector<std::pair<int64_t, int64_t>> runs;
    };
    auto slice_groups = std::vector<SliceGroup>{};
    auto seen = GroupMap<size_t>{};
    auto find_or_add = [&](GroupKey const& key) -> size_t {
      auto it = seen.find(key);
      if (it == seen.end()) {
        it = seen.emplace_hint(it, key, slice_groups.size());
        slice_groups.push_back({key, {}});
      }
      return it->second;
    };
    auto total_rows = detail::narrow<int64_t>(slice.rows());
    fill_key(0);
    auto current_key = materialize(key);
    auto current_index = find_or_add(current_key);
    auto current_begin = int64_t{0};
    for (auto row = int64_t{1}; row < total_rows; ++row) {
      fill_key(row);
      // Comparing against the current run's key avoids hashing and probing
      // for every row; a hash lookup happens only at run transitions.
      if (GroupKeyEqual{}(key, current_key)) {
        continue;
      }
      slice_groups[current_index].runs.emplace_back(current_begin, row);
      current_key = materialize(key);
      current_index = find_or_add(current_key);
      current_begin = row;
    }
    slice_groups[current_index].runs.emplace_back(current_begin, total_rows);
    // Apply the collected rows group by group. Groups are created in
    // first-seen order, matching the previous behavior.
    for (auto& sg : slice_groups) {
      auto it = groups_.find(sg.key);
      if (it == groups_.end()) {
        it = groups_.emplace_hint(it, std::move(sg.key), make_bucket(ctx));
      }
      auto rows = std::invoke([&]() -> table_slice {
        if (sg.runs.size() == 1) {
          // A single contiguous run needs no gather; slice it zero-copy.
          auto const [begin, end] = sg.runs.front();
          return subslice(slice, begin, end);
        }
        auto num_rows = int64_t{0};
        for (auto const [begin, end] : sg.runs) {
          num_rows += end - begin;
        }
        auto b = arrow::Int64Builder{arrow_memory_pool()};
        check(b.Reserve(num_rows));
        for (auto const [begin, end] : sg.runs) {
          for (auto row = begin; row < end; ++row) {
            b.UnsafeAppend(row);
          }
        }
        auto gathered = table_slice{
          check(arrow::compute::Take(to_record_batch(slice), tenzir::finish(b)))
            .record_batch(),
          slice.schema(),
        };
        gathered.import_time(slice.import_time());
        return gathered;
      });
      for (auto& aggr : it.value().aggregations) {
        aggr->update(rows, ctx);
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

  auto enrich(table_slice const& event, session ctx)
    -> std::vector<table_slice> {
    TENZIR_ASSERT(event.rows() == 1);
    if (config_.aggregates.empty()) {
      return {event};
    }
    auto group_values = std::vector<multi_series>{};
    group_values.reserve(config_.groups.size());
    for (auto& group : config_.groups) {
      group_values.push_back(eval(group.expr.inner(), event, ctx));
    }
    auto key = GroupKeyView{};
    key.reserve(config_.groups.size());
    for (auto&& group : group_values) {
      key.emplace_back(group.view3_at(0));
    }
    auto it = groups_.find(key);
    TENZIR_ASSERT(it != groups_.end());
    auto builders = std::vector<series_builder>{};
    builders.reserve(config_.aggregates.size());
    for (auto const& aggregation : it.value().aggregations) {
      auto& builder = builders.emplace_back();
      builder.data(aggregation->get());
    }
    auto values = std::vector<multi_series>{};
    values.reserve(builders.size());
    for (auto& builder : builders) {
      values.emplace_back(builder.finish());
    }
    auto result = std::vector<table_slice>{};
    for (auto parts : split_multi_series(values)) {
      TENZIR_ASSERT(parts.size() == config_.aggregates.size());
      auto output = event;
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
  auto add_aggregate = [&](Option<ast::field_path> dest,
                           ast::function_call call) {
    auto* fn = dynamic_cast<aggregation_plugin const*>(&ctx.reg().get(call));
    if (not fn) {
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

/// Validates each aggregate's arguments by calling make_aggregation with the
/// fully-resolved function-call AST. Must be called after all let-binding
/// references have been substituted so that const_eval inside the argument
/// parsers can evaluate every argument to a concrete value.
auto validate_aggregates(Config const& config, session ctx)
  -> failure_or<void> {
  for (auto const& aggr : config.aggregates) {
    auto const* fn
      = dynamic_cast<aggregation_plugin const*>(&ctx.reg().get(aggr.call));
    TENZIR_ASSERT(fn); // already verified as aggregation_plugin in build_config
    TRY(fn->make_aggregation(function_invocation{aggr.call}, ctx));
  }
  return {};
}

/// The smallest accepted timer interval. The timer catch-up loop in
/// `flush_until()` iterates once per elapsed interval, so impractically small
/// intervals would turn routine scheduling delay into unbounded work and, in
/// cumulative mode, one emitted summary per missed tick.
constexpr auto min_emit_interval = duration{std::chrono::milliseconds{10}};

/// Resolves the emission policy and aggregation mode after let substitution.
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
    return {};
  }
  if (config.emit_expr and not config.mode_expr) {
    diagnostic::error("`emit` requires `mode` to be set")
      .primary(*config.emit_expr)
      .emit(ctx);
    return failure::promise();
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
      return {};
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
      return {};
    }
    diagnostic::error("expected int or duration for `emit`")
      .primary(*config.emit_expr)
      .emit(ctx);
    return failure::promise();
  }
  if (config.mode_expr) {
    config.emission = Emission::event;
  }
  return {};
}

class Summarize final : public Operator<table_slice, table_slice> {
public:
  explicit Summarize(Config config) : state_{std::move(config)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
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

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (input.rows() == 0) {
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
  int64_t events_since_emit_ = 0;
  table_slice pending_event_;
  Option<session_provider> provider_;
  Option<steady_clock::time_point> next_flush_;
  Option<duration> restored_timer_remaining_;
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
    if (config_.legacy_frequency_expr) {
      TRY(config_.legacy_frequency_expr->substitute(ctx));
    }
    // Validate aggregation arguments and evaluate options only when
    // instantiating, i.e., when all let-bindings are guaranteed to be
    // resolved.  Both make_aggregation (for aggregates) and const_eval (for
    // option values) require fully-resolved expressions to succeed.
    if (instantiate) {
      auto provider = session_provider::make(ctx);
      TRY(validate_aggregates(config_, provider.as_session()));
      TRY(evaluate_options(config_, provider.as_session()));
    }
    return {};
  }

  auto spawn(element_type_tag input) const -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    return Summarize{config_}.with_name("summarize");
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<element_type_tag> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("operator expects events").primary(self_).emit(dh);
      return failure::promise();
    }
    return tag_v<table_slice>;
  }

  auto parallelizable() const -> bool override {
    // Without group-by keys, there is a single global aggregation state that
    // all rows must reach. Replicating the operator would yield one partial
    // result per instance. Timer emission also requires one instance because
    // replicas arm independent processing-time frontiers when they receive
    // their first rows. Counted event emission requires one instance to keep
    // a pipeline-wide event count. Final and per-event emission can use
    // hash-partitioning because it routes each group to exactly one instance.
    return not config_.groups.empty() and config_.emission != Emission::timer
           and (config_.emission != Emission::event or config_.emit_every == 1);
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

} // namespace tenzir::plugins::summarize

TENZIR_REGISTER_PLUGIN(
  (tenzir::inspection_plugin<tenzir::ir::Operator,
                             tenzir::plugins::summarize::SummarizeIr>))
