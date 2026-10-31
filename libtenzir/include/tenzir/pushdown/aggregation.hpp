//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/option.hpp"
#include "tenzir/variant.hpp"

#include <cstddef>
#include <string>
#include <vector>

/// Helpers for sources that compute an `ir::OptimizeAggregation`.
///
/// A target without nested values, or one whose nested values cannot express
/// every output that TQL allows, computes the output columns of an
/// aggregation as flat result columns and leaves the nesting to Tenzir.
namespace tenzir::pushdown {

/// The name of the result column that holds output column `index` of an
/// aggregation. A source must not use it if the table has a column of that
/// name, which its alias could shadow.
auto aggregation_column(std::size_t index) -> std::string;

/// Returns whether every function of `aggregation` is `count`, or `min` or
/// `max` with an input, which most targets can compute. Whether a target
/// computes them as TQL does depends on the types of their inputs.
auto supports_count_min_max(ir::OptimizeAggregation const& aggregation) -> bool;

/// An aggregation of group keys, `count`, `min`, and `max`, as a target
/// computes it.
///
/// The plan names each input by its path and leaves both its spelling and the
/// decision whether the target computes a column as TQL does to the target. A
/// target with typed columns resolves each input with `translate_field`, and
/// one with untyped fields names the path.
struct AggregationPlan {
  /// A group key on the field `input`.
  struct Key {
    std::vector<std::string> input;
  };

  /// `count()` without `input`, which counts events, or `count(input)`, which
  /// counts the events where the field `input` is not `null`.
  struct Count {
    Option<std::vector<std::string>> input;
  };

  /// `min(input)`.
  struct Min {
    std::vector<std::string> input;
  };

  /// `max(input)`.
  struct Max {
    std::vector<std::string> input;
  };

  /// What a result column computes.
  using Kind = variant<Key, Count, Min, Max>;

  /// A result column.
  struct Column {
    Kind kind;
    /// The name of the result column, `aggregation_column(i)` for output
    /// column `i`.
    std::string alias;
  };

  /// The result columns, one for each output column, in the same order,
  /// followed by any that `ClassifyOptions` adds.
  std::vector<Column> columns;
};

/// Options for `classify`.
struct ClassifyOptions {
  /// Whether to append a `count()` of the events of each group as the result
  /// column `aggregation_column(n)`, after the `n` output columns. A target
  /// whose semantics differ from TQL's may need it to show that its result
  /// covers every event. `nest_aggregation` ignores the column.
  bool count_events = false;
};

/// Classifies the output columns of `aggregation`, or returns `None` if
/// `supports_count_min_max` does not hold or a group key has no input.
auto classify(ir::OptimizeAggregation const& aggregation,
              ClassifyOptions options = {}) -> Option<AggregationPlan>;

/// Builds the output events of `aggregation` from `events`, whose fields are
/// the result columns named by `aggregation_column`. The output fields follow
/// the order of `aggregation.columns`, as the summary does, and the events
/// carry `aggregation.schema_name`.
auto nest_aggregation(nova::Events events,
                      ir::OptimizeAggregation const& aggregation,
                      diagnostic_handler& dh) -> nova::Events;

/// Returns whether a source may run `aggregation.fallback` in a subpipeline,
/// or emits the error that the fallback itself would emit.
///
/// The executor cannot checkpoint an operator with subpipelines yet, and the
/// `summarize` of the fallback rejects checkpointing anyway. Without this
/// check, a checkpoint would reach the source while the subpipeline runs,
/// rather than the pipeline failing as it would without the optimization.
auto can_aggregate_locally(ir::OptimizeAggregation const& aggregation,
                           OpCtx& ctx) -> bool;

} // namespace tenzir::pushdown
