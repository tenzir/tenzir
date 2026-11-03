//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/aggregation.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/eval_util.hpp"
#include "tenzir/pushdown/layout.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/try.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <vector>

namespace tenzir::pushdown {

namespace {

/// Classifies a column of an aggregation that `supports_count_min_max`
/// accepts.
auto classify_column(ir::OptimizeAggregation::Column const& column)
  -> Option<AggregationPlan::Kind> {
  if (not column.function) {
    if (not column.input) {
      return None{};
    }
    return AggregationPlan::Key{*column.input};
  }
  auto const& function = *column.function;
  if (function == "count") {
    return AggregationPlan::Count{column.input};
  }
  TENZIR_ASSERT(column.input);
  if (function == "min") {
    return AggregationPlan::Min{*column.input};
  }
  TENZIR_ASSERT(function == "max");
  return AggregationPlan::Max{*column.input};
}

} // namespace

auto aggregation_column(std::size_t index) -> std::string {
  return fmt::format("__tenzir_aggregate_{}", index);
}

auto supports_count_min_max(ir::OptimizeAggregation const& aggregation)
  -> bool {
  return std::ranges::all_of(
    aggregation.columns, [](ir::OptimizeAggregation::Column const& column) {
      if (not column.function) {
        return true;
      }
      auto const& function = *column.function;
      return function == "count"
             or ((function == "min" or function == "max") and column.input);
    });
}

auto classify(ir::OptimizeAggregation const& aggregation,
              ClassifyOptions options) -> Option<AggregationPlan> {
  if (not supports_count_min_max(aggregation)) {
    return None{};
  }
  auto result = AggregationPlan{};
  for (auto i = std::size_t{0}; i < aggregation.columns.size(); ++i) {
    TRY(auto kind, classify_column(aggregation.columns[i]));
    result.columns.push_back({
      .kind = std::move(kind),
      .alias = aggregation_column(i),
    });
  }
  if (options.count_events) {
    result.columns.push_back({
      .kind = AggregationPlan::Count{None{}},
      .alias = aggregation_column(aggregation.columns.size()),
    });
  }
  return result;
}

auto nest_aggregation(nova::Events events,
                      ir::OptimizeAggregation const& aggregation,
                      diagnostic_handler& dh) -> nova::Events {
  auto layout = Layout{
    .fields = LayoutFields{},
    .hidden = {},
    .schema_name = aggregation.schema_name,
  };
  for (auto i = std::size_t{0}; i < aggregation.columns.size(); ++i) {
    layout.fields->emplace_back(aggregation.columns[i].output,
                                aggregation_column(i));
  }
  return apply_layout(std::move(events), layout, dh);
}

auto can_aggregate_locally(ir::OptimizeAggregation const& aggregation,
                           OpCtx& ctx) -> bool {
  if (not ctx.checkpoint_settings()) {
    return true;
  }
  auto const source
    = aggregation.fallback.operators.empty()
        ? location::unknown
        : aggregation.fallback.operators.back()->main_location();
  diagnostic::error(
    "`summarize` does not support checkpointing with `--nova` yet")
    .primary(source)
    .emit(ctx);
  return false;
}

} // namespace tenzir::pushdown
