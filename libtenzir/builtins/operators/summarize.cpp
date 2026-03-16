//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/arrow_time_utils.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/async.hpp>
#include <tenzir/async/task.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/core.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/detail/weak_run_delayed.hpp>
#include <tenzir/error.hpp>
#include <tenzir/hash/hash_append.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/parser_interface.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/session.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/registry.hpp>
#include <tenzir/type.hpp>

#include <arrow/compute/api_scalar.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <caf/expected.hpp>
#include <tsl/robin_map.h>

#include <algorithm>
#include <ranges>
#include <utility>
#include <variant>

namespace tenzir::plugins::summarize {

namespace {

/// The key by which aggregations are grouped. Essentially, this is a vector of
/// data. We create a new type here to support a custom hash and equality
/// operation to support lookups with non-materialized keys.
struct group_by_key : std::vector<data> {
  using vector::vector;
};

/// A view on a group-by key.
struct group_by_key_view : std::vector<data_view> {
  using vector::vector;

  /// Materializes a view on a group-by key.
  /// @param views The group-by key view to materialize.
  friend group_by_key materialize(const group_by_key_view& views) {
    auto result = group_by_key{};
    result.reserve(views.size());
    for (const auto& view : views) {
      result.push_back(materialize(view));
    }
    return result;
  }
};

/// The hash functor for enabling use of *group_by_key* as a key in unordered
/// map data structures with transparent lookup.
struct group_by_key_hash {
  size_t operator()(const group_by_key& x) const noexcept {
    auto hasher = xxh64{};
    for (const auto& value : x) {
      hash_append(hasher, make_view(value));
    }
    return hasher.finish();
  }

  size_t operator()(const group_by_key_view& x) const noexcept {
    auto hasher = xxh64{};
    for (const auto& value : x) {
      hash_append(hasher, value);
    }
    return hasher.finish();
  }
};

/// The equality functor for enabling use of *group_by_key* as a key in
/// unordered map data structures with transparent lookup.
struct group_by_key_equal {
  using is_transparent = void;

  bool
  operator()(const group_by_key_view& x, const group_by_key& y) const noexcept {
    return std::equal(x.begin(), x.end(), y.begin(), y.end(),
                      [](const auto& lhs, const auto& rhs) {
                        return lhs == make_view(rhs);
                      });
  }

  bool
  operator()(const group_by_key& x, const group_by_key_view& y) const noexcept {
    return std::equal(x.begin(), x.end(), y.begin(), y.end(),
                      [](const auto& lhs, const auto& rhs) {
                        return make_view(lhs) == rhs;
                      });
  }

  bool operator()(const group_by_key& x, const group_by_key& y) const noexcept {
    return x == y;
  }

  bool operator()(const group_by_key_view& x,
                  const group_by_key_view& y) const noexcept {
    return x == y;
  }
};

struct aggregate_t {
  std::optional<ast::field_path> dest;
  ast::function_call call;

  friend auto inspect(auto& f, aggregate_t& x) -> bool {
    return f.object(x).fields(f.field("dest", x.dest), f.field("call", x.call));
  }
};

struct group_t {
  std::optional<ast::field_path> dest;
  ast::field_path expr;

  friend auto inspect(auto& f, group_t& x) -> bool {
    return f.object(x).fields(f.field("dest", x.dest), f.field("expr", x.expr));
  }
};

struct config {
  std::vector<aggregate_t> aggregates;
  std::vector<group_t> groups;

  /// Because we allow mixing aggregates and groups and want to emit them in the
  /// same order, we need to store some additional information, unless we use
  /// something like `vector<variant<aggregate_t, ast::selector>>` instead. But
  /// that makes it more tricky to `zip`. If the index is positive, it
  /// corresponds to `aggregates`, otherwise `groups[-index - 1]`.
  std::vector<int64_t> indices;

  /// Unevaluated expression for the `frequency` option. Set by build_config
  /// when parsing options={frequency: <expr>}. Evaluated and written to
  /// `frequency` by evaluate_options() after let-bindings are substituted.
  std::optional<ast::expression> frequency_expr;

  /// Unevaluated expression for the `mode` option. Set by build_config when
  /// parsing options={mode: <expr>}. Evaluated and written to `mode` by
  /// evaluate_options() after let-bindings are substituted.
  std::optional<ast::expression> mode_expr;

  /// Optional frequency for periodic emission of aggregation results.
  /// Populated by evaluate_options().
  std::optional<duration> frequency;

  /// Emission mode: "reset", "cumulative", or "update".
  /// Populated by evaluate_options(). Defaults to "reset".
  std::string mode = "reset";

  friend auto inspect(auto& f, config& x) -> bool {
    return f.object(x).fields(f.field("aggregates", x.aggregates),
                              f.field("groups", x.groups),
                              f.field("indices", x.indices),
                              f.field("frequency_expr", x.frequency_expr),
                              f.field("mode_expr", x.mode_expr),
                              f.field("frequency", x.frequency),
                              f.field("mode", x.mode));
  }
};

template <class Value>
using group_map
  = tsl::robin_map<group_by_key, Value, group_by_key_hash, group_by_key_equal>;

struct bucket2 {
  std::vector<std::unique_ptr<aggregation_instance>> aggregations{};
  std::vector<chunk_ptr>
    blobs_{}; // staging area for restore; empty during normal use

  friend auto inspect(auto& f, bucket2& x) -> bool {
    if constexpr (std::remove_reference_t<decltype(f)>::is_loading) {
      return f.apply(x.blobs_);
    } else {
      auto blobs = std::vector<chunk_ptr>{};
      blobs.reserve(x.aggregations.size());
      for (const auto& aggr : x.aggregations) {
        blobs.push_back(aggr->save());
      }
      return f.apply(blobs);
    }
  }
};

class implementation2 {
public:
  explicit implementation2(const config& cfg, session ctx)
    : cfg_{cfg}, ctx_{ctx} {
  }

  auto make_bucket() -> bucket2 {
    auto bucket = bucket2{};
    for (const auto& aggr : cfg_.aggregates) {
      // We already checked the cast and instantiation before.
      const auto* fn
        = dynamic_cast<const aggregation_plugin*>(&ctx_.reg().get(aggr.call));
      TENZIR_ASSERT(fn);
      bucket.aggregations.push_back(
        fn->make_aggregation(aggregation_plugin::invocation{aggr.call}, ctx_)
          .unwrap());
    }
    return bucket;
  }

  void add(const table_slice& slice) {
    saw_input_ = true;
    auto group_values = std::vector<multi_series>{};
    for (auto& group : cfg_.groups) {
      group_values.push_back(eval(group.expr.inner(), slice, ctx_));
    }
    auto key = group_by_key_view{};
    key.resize(cfg_.groups.size());
    auto update_group = [&](bucket2& group, int64_t begin, int64_t end) {
      for (auto&& aggr : group.aggregations) {
        aggr->update(subslice(slice, begin, end), ctx_);
      }
    };
    auto find_or_create_group = [&](int64_t row) -> bucket2* {
      TENZIR_ASSERT(key.size() == group_values.size());
      for (auto&& [key_value, group] : std::views::zip(key, group_values)) {
        key_value = group.value_at(row);
      }
      auto it = groups_.find(key);
      if (it == groups_.end()) {
        it = groups_.emplace_hint(it, materialize(key), make_bucket());
      }
      return &it.value();
    };
    auto total_rows = detail::narrow<int64_t>(slice.rows());
    auto current_group = find_or_create_group(0);
    auto current_begin = int64_t{0};
    for (auto row = int64_t{1}; row < total_rows; ++row) {
      auto group = find_or_create_group(row);
      if (current_group != group) {
        update_group(*current_group, current_begin, row);
        current_group = group;
        current_begin = row;
      }
    }
    update_group(*current_group, current_begin, total_rows);
  }

  auto flush() -> std::vector<table_slice> {
    return flush(false);
  }

  auto flush(bool force) -> std::vector<table_slice> {
    // Avoid emitting before any input arrived unless explicitly forced (used
    // for final emission).
    if (not force and not saw_input_) {
      return {};
    }
    if (cfg_.mode == "reset") {
      // Emit all groups and reset aggregations
      auto result = finish_impl();
      for (auto it = groups_.begin(); it != groups_.end(); ++it) {
        auto& bucket = it.value();
        bucket.aggregations.clear();
        for (const auto& aggr : cfg_.aggregates) {
          const auto* fn = dynamic_cast<const aggregation_plugin*>(
            &ctx_.reg().get(aggr.call));
          TENZIR_ASSERT(fn);
          bucket.aggregations.push_back(
            fn->make_aggregation(aggregation_plugin::invocation{aggr.call},
                                 ctx_)
              .unwrap());
        }
      }
      return result;
    }
    if (cfg_.mode == "cumulative") {
      // Emit all groups and keep aggregations
      return finish_impl();
    }
    TENZIR_ASSERT(cfg_.mode == "update");
    // Emit only groups where values changed
    auto b = series_builder{};
    for (const auto& [key, group] : groups_) {
      // Get current aggregation values.
      auto current_values = std::vector<data>{};
      current_values.reserve(group.aggregations.size());
      for (const auto& aggr : group.aggregations) {
        current_values.push_back(aggr->get());
      }
      // Check if values changed (or first emission for this group).
      auto it = previous_values_.find(key);
      auto should_emit
        = (it == previous_values_.end()) or (it->second != current_values);
      if (should_emit) {
        b.data(finish_group(key, group));
        previous_values_[key] = current_values;
      }
    }
    // Special case: if there are no configured groups, and no groups were
    // created because we didn't get any input events.
    if (cfg_.groups.empty() and groups_.empty()) {
      b.data(finish_group(group_by_key{}, make_bucket()));
    }
    return b.finish_as_table_slice();
  }

  auto finish() -> std::vector<table_slice> {
    if (cfg_.mode == "update") {
      // Reuse flush to honor change detection for the final emission.
      return flush(true);
    }
    return finish_impl();
  }

  auto take_restore_failed() noexcept -> bool {
    return std::exchange(restore_failed_, false);
  }

  auto discard_snapshot(bool signal_failure = false) noexcept -> void {
    groups_.clear();
    previous_values_.clear();
    saw_input_ = false;
    restore_failed_ = signal_failure;
  }

private:
  auto finish_impl() -> std::vector<table_slice> {
    // Special case: if there are no configured groups, and no groups were
    // created because we didn't get any input events, then we create a new
    // bucket and just finish it. That way, `from [] | summarize count()` will
    // return a single event showing a count of zero.
    if (cfg_.groups.empty() and groups_.empty()) {
      auto b = series_builder{};
      b.data(finish_group(group_by_key{}, make_bucket()));
      return b.finish_as_table_slice();
    }
    // TODO: Group by schema again to make this more efficient.
    auto b = series_builder{};
    for (const auto& [key, group] : groups_) {
      b.data(finish_group(key, group));
    }
    return b.finish_as_table_slice();
  }

  /// Writes @p value into @p root at the path described by @p sel.
  static auto
  emplace_value(record& root, const ast::field_path& sel, data value) -> void {
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
    for (const auto& segment : sel.path()) {
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
  auto finish_group(const group_by_key& key, const bucket2& bucket) const
    -> record {
    auto result = record{};
    for (auto index : cfg_.indices) {
      if (index >= 0) {
        const auto& dest = cfg_.aggregates[index].dest;
        auto value = bucket.aggregations[index]->get();
        if (dest) {
          emplace_value(result, *dest, value);
        } else {
          const auto& call = cfg_.aggregates[index].call;
          // TODO: Decide and properly implement this. The format below
          // produces names like `count()` or `sum(x)`.  It is wrong for
          // field names that contain special characters (e.g. spaces or
          // dots), because the segments are joined with '.' without quoting.
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
            auto s = std::string{};
            if (sel->has_this()) {
              s = "this";
            }
            for (const auto& segment : sel->path()) {
              // TODO: This is wrong if the path contains special characters.
              if (not s.empty()) {
                s += '.';
              }
              s += segment.id.name;
            }
            return s;
          });
          result.emplace(fmt::format("{}({})", call.fn.path[0].name, arg),
                         value);
        }
      } else {
        auto group_index = -index - 1;
        const auto& group_def = cfg_.groups[group_index];
        const auto& dest = group_def.dest ? *group_def.dest : group_def.expr;
        emplace_value(result, dest, key[group_index]);
      }
    }
    return result;
  }

  friend auto inspect(auto& f, implementation2& x) -> bool {
    auto on_load = [&]() noexcept {
      const auto expected = x.make_bucket().aggregations.size();
      for (auto it = x.groups_.begin(); it != x.groups_.end(); ++it) {
        auto& bucket = it.value();
        if (bucket.blobs_.size() != expected) {
          TENZIR_WARN("summarize: snapshot has {} aggregation(s) but pipeline "
                      "expects {}; discarding snapshot and starting fresh",
                      bucket.blobs_.size(), expected);
          x.discard_snapshot();
          return true;
        }
        auto fresh = x.make_bucket();
        for (auto i = size_t{0}; i < bucket.blobs_.size(); ++i) {
          if (not fresh.aggregations[i]->restore(std::move(bucket.blobs_[i]))) {
            TENZIR_WARN("summarize: failed to restore aggregation state; "
                        "discarding snapshot and starting fresh");
            x.discard_snapshot(/*signal_failure=*/true);
            return true;
          }
        }
        bucket.aggregations = std::move(fresh.aggregations);
        bucket.blobs_.clear();
      }
      return true;
    };
    return f.object(x).on_load(on_load).fields(
      f.field("saw_input", x.saw_input_), f.field("groups", x.groups_),
      f.field("previous_values", x.previous_values_));
  }

  const config& cfg_;
  session ctx_;
  group_map<bucket2> groups_;
  bool saw_input_ = false;
  bool restore_failed_ = false;
  /// Previous aggregation values for each group (used in "update" mode)
  group_map<std::vector<data>> previous_values_;
};

// ---------------------------------------------------------------------------
// build_config: shared parsing logic used by both plugin2::make() and Summarize
// ---------------------------------------------------------------------------

/// Classifies a flat list of expressions (as received from inv.args or from
/// optional_variadic) into a fully-populated config.  The options={...}
/// positional-assignment syntax is handled here, identical to the old make().
auto build_config(std::vector<ast::expression> exprs, session ctx)
  -> failure_or<config> {
  auto cfg = config{};
  auto failed = false;

  // Store option field expressions without evaluating them.  Evaluation is
  // intentionally deferred to evaluate_options(), which is called from
  // plugin2::make() immediately (non-IR path, let-bindings already resolved)
  // and from summarize_ir::substitute() after substitution (IR path).  This
  // mirrors the pattern used for aggregation arguments: build_config stores
  // the AST, a separate step validates/evaluates it once bindings are known.
  auto parse_options = [&](const ast::record& rec) {
    for (const auto& item : rec.items) {
      const auto* field = try_as<ast::record::field>(item);
      if (not field) {
        diagnostic::error("spread not allowed in options record")
          .primary(rec.get_location())
          .emit(ctx);
        failed = true;
        return;
      }
      const auto& name = field->name.name;
      if (name == "frequency") {
        cfg.frequency_expr = field->expr;
      } else if (name == "mode") {
        cfg.mode_expr = field->expr;
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
    = [&](std::optional<ast::field_path> dest, ast::function_call call) {
        auto fn = dynamic_cast<const aggregation_plugin*>(&ctx.reg().get(call));
        if (not fn) {
          diagnostic::error("function does not support aggregations")
            .primary(call.fn)
            .hint("if you want to group by this, use assignment before")
            .docs("https://docs.tenzir.com/operators/summarize")
            .emit(ctx);
          failed = true;
          return;
        }
        // Argument validation via make_aggregation is intentionally deferred:
        // args may contain unresolved let-bindings when called from compile().
        // Validation is performed by validate_aggregates(), which is called
        // directly in plugin2::make() and in summarize_ir::substitute() after
        // substitution has resolved all let-binding references.
        auto index = detail::narrow<int64_t>(cfg.aggregates.size());
        cfg.indices.push_back(index);
        cfg.aggregates.emplace_back(std::move(dest), std::move(call));
      };

  auto add_group
    = [&](std::optional<ast::field_path> dest, ast::field_path expr) {
        auto index = -detail::narrow<int64_t>(cfg.groups.size()) - 1;
        cfg.indices.push_back(index);
        cfg.groups.emplace_back(std::move(dest), std::move(expr));
      };

  for (auto& arg : exprs) {
    arg.match(
      [&](ast::function_call& arg) {
        add_aggregate(std::nullopt, std::move(arg));
      },
      [&](ast::assignment& arg) {
        auto* left = try_as<ast::field_path>(arg.left);
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
          add_group(std::nullopt, std::move(*selector));
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
  return cfg;
}

/// Validates each aggregate's arguments by calling make_aggregation with the
/// fully-resolved function-call AST. Must be called after all let-binding
/// references have been substituted so that const_eval inside the argument
/// parsers can evaluate every argument to a concrete value.
auto validate_aggregates(const config& cfg, session ctx) -> failure_or<void> {
  for (const auto& aggr : cfg.aggregates) {
    const auto* fn
      = dynamic_cast<const aggregation_plugin*>(&ctx.reg().get(aggr.call));
    TENZIR_ASSERT(fn); // already verified as aggregation_plugin in build_config
    TRY(fn->make_aggregation(aggregation_plugin::invocation{aggr.call}, ctx));
  }
  return {};
}

/// Evaluates cfg.frequency_expr and cfg.mode_expr and writes the results into
/// cfg.frequency and cfg.mode. Must be called after all let-binding references
/// in the expressions have been substituted. Mirrors validate_aggregates() for
/// the options field: build_config stores the AST, this step evaluates it.
auto evaluate_options(config& cfg, session ctx) -> failure_or<void> {
  if (cfg.frequency_expr) {
    TRY(auto value, const_eval(*cfg.frequency_expr, ctx));
    auto* dur = try_as<duration>(value);
    if (not dur) {
      diagnostic::error("expected duration for `frequency`")
        .primary(*cfg.frequency_expr)
        .emit(ctx);
      return failure::promise();
    }
    cfg.frequency = *dur;
  }
  if (cfg.mode_expr) {
    TRY(auto value, const_eval(*cfg.mode_expr, ctx));
    auto* str = try_as<std::string>(value);
    if (not str) {
      diagnostic::error("expected string for `mode`")
        .primary(*cfg.mode_expr)
        .emit(ctx);
      return failure::promise();
    }
    if (*str == "reset" || *str == "cumulative" || *str == "update") {
      cfg.mode = *str;
    } else {
      diagnostic::error("invalid mode `{}`", *str)
        .primary(*cfg.mode_expr)
        .hint("expected `reset`, `cumulative`, or `update`")
        .emit(ctx);
      return failure::promise();
    }
  }
  if (cfg.mode_expr and not cfg.frequency) {
    diagnostic::error("`mode` requires `frequency` to be set")
      .primary(*cfg.mode_expr)
      .emit(ctx);
    return failure::promise();
  }
  return {};
}

class summarize_operator2 final : public crtp_operator<summarize_operator2> {
public:
  summarize_operator2() = default;

  explicit summarize_operator2(config cfg) : cfg_{std::move(cfg)} {
  }

  auto name() const -> std::string override {
    return "tql2.summarize";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // TODO: Do not create a new session here.
    auto provider = session_provider::make(ctrl.diagnostics());
    auto impl = implementation2{cfg_, provider.as_session()};

    if (cfg_.frequency) {
      // Periodic emission mode.
      auto pending_flush = false;
      detail::weak_run_delayed_loop(
        &ctrl.self(), *cfg_.frequency,
        [&] {
          pending_flush = true;
          ctrl.set_waiting(false);
        },
        false);
      for (auto slice : input) {
        // Drain pending flushes that were scheduled while idle.
        if (std::exchange(pending_flush, false)) {
          for (auto result : impl.flush()) {
            co_yield std::move(result);
          }
        }
        if (slice.rows() == 0) {
          co_yield {};
        } else {
          impl.add(slice);
        }
      }
      // Flush anything that may have been scheduled while consuming the last
      // slices before producing the final result.
      if (std::exchange(pending_flush, false)) {
        for (auto result : impl.flush()) {
          co_yield std::move(result);
        }
      }
      // Final emission when input ends.
      for (auto result : impl.finish()) {
        co_yield std::move(result);
      }
    } else {
      for (auto&& slice : input) {
        if (slice.rows() == 0) {
          co_yield {};
          continue;
        }
        impl.add(slice);
      }
      for (auto slice : impl.finish()) {
        co_yield std::move(slice);
      }
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

  friend auto inspect(auto& f, summarize_operator2& x) -> bool {
    return f.apply(x.cfg_);
  }

private:
  config cfg_;
};

class Summarize final : public Operator<table_slice, table_slice> {
public:
  explicit Summarize(config cfg) : cfg_{std::move(cfg)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    provider_.emplace(session_provider::make(ctx.dh()));
    impl_ = std::make_unique<implementation2>(cfg_, provider_->as_session());
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(ctx);
    if (impl_ && impl_->take_restore_failed()) {
      diagnostic::warning("summarize aggregation state could not be restored "
                          "from snapshot; restarting from empty state")
        .emit(provider_->as_session());
    }
    co_await flush(push);
    if (input.rows() == 0) {
      co_return;
    }
    impl_->add(input);
    co_return;
  }

  auto finalize(Push<table_slice>& push, OpCtx& ctx)
    -> Task<FinalizeBehavior> override {
    TENZIR_UNUSED(ctx);
    co_await flush(push);
    for (auto& slice : impl_->finish()) {
      co_await push(std::move(slice));
    }
    co_return FinalizeBehavior::done;
  }

  /// Drives periodic flushing when `options={frequency: ...}` is set.
  /// Sleeps until the next scheduled wall-clock deadline and marks a pending
  /// flush. If process() runs before the scheduled task executes, it will flush
  /// immediately and clear the pending flag so process_task() can skip it.
  /// Without a frequency the default implementation waits forever.
  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (not cfg_.frequency) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (next_flush_ == time::min()) {
      next_flush_ = time::clock::now() + *cfg_.frequency;
    } else {
      next_flush_ += *cfg_.frequency;
    }
    co_await sleep_until(next_flush_);
    pending_flush_ = true;
    co_return {};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    TENZIR_UNUSED(result, ctx);
    co_await flush(push);
  }

  auto snapshot(Serde& serde) -> void override {
    if (impl_) {
      serde("state", *impl_);
    }
  }

private:
  // Push a summary, if a pending_flush_ is set.
  // This only happen if frequency is set.
  auto flush(Push<table_slice>& push) -> Task<void> {
    if (not std::exchange(pending_flush_, false)) {
      co_return;
    }
    for (auto& slice : impl_->flush()) {
      co_await push(std::move(slice));
    }
  }

  config cfg_;
  // provider_ must outlive impl_ because impl_ holds a session that
  // references provider_.  Member destruction order (reverse of declaration)
  // guarantees impl_ is destroyed before provider_.
  std::optional<session_provider> provider_;
  std::unique_ptr<implementation2> impl_;
  /// Next wall-clock deadline for periodic flush. Mutable because await_task()
  /// is const but advances the schedule each time it is awaited.
  mutable time next_flush_ = time::min();
  /// Set when await_task's sleep completes. If process() runs before
  /// process_task(), it will perform the flush and clear this flag so the
  /// scheduled task can skip its flush.
  mutable bool pending_flush_ = false;
};

class summarize_ir final : public ir::Operator {
public:
  summarize_ir() = default;

  summarize_ir(location self, config cfg) : self_{self}, cfg_{std::move(cfg)} {
  }

  auto name() const -> std::string override {
    return "summarize";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    // Substitute through function-call arguments in aggregates; they can
    // reference let-bindings.  Group field-paths are static identifiers.
    for (auto& aggregate : cfg_.aggregates) {
      for (auto& arg : aggregate.call.args) {
        TRY(arg.substitute(ctx));
      }
    }
    // Substitute through option expressions so that let-binding references
    // (e.g. options={frequency: $freq}) are resolved before evaluation.
    if (cfg_.frequency_expr) {
      TRY(cfg_.frequency_expr->substitute(ctx));
    }
    if (cfg_.mode_expr) {
      TRY(cfg_.mode_expr->substitute(ctx));
    }
    // Validate aggregation arguments and evaluate options only when
    // instantiating, i.e., when all let-bindings are guaranteed to be
    // resolved.  Both make_aggregation (for aggregates) and const_eval (for
    // option values) require fully-resolved expressions to succeed.
    if (instantiate) {
      auto provider = session_provider::make(ctx);
      TRY(validate_aggregates(cfg_, provider.as_session()));
      TRY(evaluate_options(cfg_, provider.as_session()));
    }
    return {};
  }

  auto spawn(element_type_tag input) && -> AnyOperator override {
    TENZIR_ASSERT(input.is<table_slice>());
    return Summarize{std::move(cfg_)};
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("operator expects events").primary(self_).emit(dh);
      return failure::promise();
    }
    return tag_v<table_slice>;
  }

  auto main_location() const -> location override {
    return self_;
  }

  friend auto inspect(auto& f, summarize_ir& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_), f.field("cfg", x.cfg_));
  }

private:
  location self_;
  config cfg_;
};

class plugin2 final : public virtual operator_plugin2<summarize_operator2>,
                      public virtual operator_compiler_plugin {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    TRY(auto cfg, build_config(std::move(inv.args), ctx));
    TRY(validate_aggregates(cfg, ctx));
    TRY(evaluate_options(cfg, ctx));
    return std::make_unique<summarize_operator2>(std::move(cfg));
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<Box<ir::Operator>> override {
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
    TRY(auto cfg, build_config(std::move(inv.args), provider.as_session()));
    return summarize_ir{loc, std::move(cfg)};
  }
};

} // namespace

} // namespace tenzir::plugins::summarize

TENZIR_REGISTER_PLUGIN(tenzir::plugins::summarize::plugin2)
TENZIR_REGISTER_PLUGIN(
  (tenzir::inspection_plugin<tenzir::ir::Operator,
                             tenzir::plugins::summarize::summarize_ir>))
