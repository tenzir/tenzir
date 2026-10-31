//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/sql.hpp"

#include "tenzir/pushdown/aggregation.hpp"
#include "tenzir/pushdown/read.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <utility>

namespace tenzir::pushdown {

namespace {

/// The value of an input field, as the target computes it.
struct Input {
  FieldValue field;
  /// The rendered expression of the value.
  std::string value;
};

/// Selects the visible `columns` that `projection`, the predicates in
/// `local`, and `keep` need, in table order, or `*` if they need every field.
auto select_columns(std::span<SqlColumn const> columns,
                    ir::OptimizeFilter const& local,
                    Option<ir::OptimizeProjection> const& projection,
                    std::span<std::string const> keep,
                    SqlQueryDialect const& dialect) -> std::string {
  auto wanted = read_projection(projection, local);
  if (not wanted) {
    return "*";
  }
  wanted->insert(wanted->end(), keep.begin(), keep.end());
  auto selected = std::vector<std::string>{};
  for (auto const& column : columns) {
    if (column.visible and std::ranges::contains(*wanted, column.name)) {
      selected.push_back(dialect.quote_identifier(column.name));
    }
  }
  if (selected.empty()) {
    auto first = std::ranges::find(columns, true, &SqlColumn::visible);
    if (first == columns.end()) {
      return "*";
    }
    selected.push_back(dialect.quote_identifier(first->name));
  }
  return fmt::to_string(fmt::join(selected, ", "));
}

} // namespace

auto SqlQueryDialect::limit_clause() const -> LimitClause {
  return LimitClause::limit;
}

auto SqlQueryDialect::max_limit() const -> uint64_t {
  return std::numeric_limits<uint64_t>::max();
}

auto render_select(SqlSelect const& select, SqlQueryDialect const& dialect)
  -> std::string {
  auto const top = dialect.limit_clause() == LimitClause::top;
  auto result = std::string{"SELECT "};
  if (top and select.limit) {
    fmt::format_to(std::back_inserter(result), "TOP ({}) ", *select.limit);
  }
  fmt::format_to(std::back_inserter(result), "{} FROM {}", select.selection,
                 select.from);
  if (not select.where.empty()) {
    fmt::format_to(std::back_inserter(result), " WHERE {}",
                   fmt::join(select.where, " AND "));
  }
  if (not select.group_by.empty()) {
    fmt::format_to(std::back_inserter(result), " GROUP BY {}",
                   fmt::join(select.group_by, ", "));
  }
  if (not top and select.limit) {
    fmt::format_to(std::back_inserter(result), " LIMIT {}", *select.limit);
  }
  return result;
}

auto make_table_plan(std::span<SqlColumn const> columns,
                     ColumnModel const& model, Renderer const& renderer,
                     SqlQueryDialect const& dialect, ir::OptimizeFilter filter,
                     Option<ir::OptimizeProjection> const& projection,
                     registry const& reg, std::span<std::string const> keep)
  -> TablePlan {
  if (columns.empty()) {
    return TablePlan{
      .selection = "*",
      .pushed = {},
      .local = std::move(filter),
    };
  }
  auto split = split_filter(std::move(filter), model, renderer, reg);
  return TablePlan{
    .selection
    = select_columns(columns, split.remaining, projection, keep, dialect),
    .pushed = std::move(split.pushed),
    .local = std::move(split.remaining),
  };
}

auto make_table_select(TablePlan const& plan, std::string from,
                       Option<uint64_t> limit, SqlQueryDialect const& dialect)
  -> SqlSelect {
  return SqlSelect{
    .selection = plan.selection,
    .from = std::move(from),
    .where = plan.pushed,
    .group_by = {},
    // A target that rejects a limit fails the query, so the operator enforces
    // such a limit alone. A limit of zero is fine in SQL.
    .limit = pushable_limit(plan.local, limit, dialect.max_limit(), 0),
  };
}

auto make_aggregate_select(AggregationPlan const& plan,
                           ColumnModel const& columns,
                           SqlQueryDialect const& dialect,
                           SqlAggregateDialect const& aggregates,
                           std::string from, std::span<std::string const> where)
  -> Option<SqlSelect> {
  if (plan.columns.empty()) {
    return None{};
  }
  auto resolve = [&](std::span<std::string const> path) -> Option<Input> {
    TRY(auto field, translate_field(path, columns));
    TRY(auto value, aggregates.render_value(field.expr));
    return Input{std::move(field), std::move(value)};
  };
  auto selection = std::vector<std::string>{};
  auto group_by = std::vector<std::string>{};
  for (auto const& column : plan.columns) {
    // An alias that names a column could change what the query reads.
    if (aggregates.alias_collides(column.alias)) {
      return None{};
    }
    TRY(auto value,
        match(
          column.kind,
          [&](AggregationPlan::Key const& key) -> Option<std::string> {
            TRY(auto input, resolve(key.input));
            TRY(auto result,
                aggregates.render_key(key.input, input.field, input.value));
            group_by.push_back(std::move(input.value));
            return result;
          },
          [&](AggregationPlan::Count const& count) -> Option<std::string> {
            if (not count.input) {
              return aggregates.render_count(None{});
            }
            TRY(auto input, resolve(*count.input));
            return aggregates.render_count(input.value);
          },
          [&](AggregationPlan::Min const& min) -> Option<std::string> {
            TRY(auto input, resolve(min.input));
            return aggregates.render_extremum(Extremum::min, input.field,
                                              input.value);
          },
          [&](AggregationPlan::Max const& max) -> Option<std::string> {
            TRY(auto input, resolve(max.input));
            return aggregates.render_extremum(Extremum::max, input.field,
                                              input.value);
          }));
    selection.push_back(
      fmt::format("{} AS {}", value, dialect.quote_identifier(column.alias)));
  }
  return SqlSelect{
    .selection = fmt::to_string(fmt::join(selection, ", ")),
    .from = std::move(from),
    .where = {where.begin(), where.end()},
    .group_by = std::move(group_by),
    .limit = None{},
  };
}

} // namespace tenzir::pushdown
