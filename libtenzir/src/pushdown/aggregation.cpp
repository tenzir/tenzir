//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/aggregation.hpp"

#include "tenzir/nova/eval_util.hpp"
#include "tenzir/tql2/ast.hpp"

#include <fmt/format.h>

#include <vector>

namespace tenzir::pushdown {

auto aggregation_column(std::size_t index) -> std::string {
  return fmt::format("__tenzir_aggregate_{}", index);
}

auto nest_aggregation(nova::Events events,
                      ir::OptimizeAggregation const& aggregation,
                      diagnostic_handler& dh) -> nova::Events {
  auto const length = events.length();
  auto result = nova::Array<nova::Record>::make_empty(length);
  for (auto i = std::size_t{0}; i < aggregation.columns.size(); ++i) {
    auto value = events.data.field(aggregation_column(i));
    // A reader may drop a column that it cannot decode, which it warns
    // about. The field is then missing, as for a missing input field.
    if (not value) {
      continue;
    }
    auto path = std::vector<ast::field_path::segment>{};
    for (auto const& name : aggregation.columns[i].output) {
      path.push_back({
        .id = ast::identifier{name, location::unknown},
        .has_question_mark = false,
      });
    }
    result = nova::assign_nested_field(std::move(result), path,
                                       std::move(*value), dh);
  }
  return nova::Events{
    std::move(result),
    std::move(events.mask),
    nova::Events::Meta::make_empty(length, aggregation.schema_name),
  };
}

auto can_aggregate_locally(ir::OptimizeAggregation const& aggregation,
                           OpCtx& ctx) -> bool {
  if (not ctx.checkpoint_settings()) {
    return true;
  }
  auto const source
    = aggregation.fallback.operators.empty()
        ? location::unknown
        : aggregation.fallback.operators.front()->main_location();
  diagnostic::error(
    "`summarize` does not support checkpointing with `--nova` yet")
    .primary(source)
    .emit(ctx);
  return false;
}

} // namespace tenzir::pushdown
