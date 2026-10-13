//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/events.hpp"

#include <cstddef>
#include <string>

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
