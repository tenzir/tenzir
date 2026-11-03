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
#include "tenzir/ref.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <cmath>
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

auto SqlQueryDialect::quote_string(std::string_view text) const
  -> Option<std::string> {
  auto result = std::string{"'"};
  for (auto c : text) {
    result += c;
    if (c == '\'') {
      result += '\'';
    }
  }
  result += '\'';
  return result;
}

auto SqlQueryDialect::render_double(double value) const -> std::string {
  // Without an exponent, `2.0` would read as an integer, and `0.1` as a
  // `DECIMAL` in many dialects.
  auto result = fmt::format("{}", value);
  if (result.find_first_of("eE") == std::string::npos) {
    result += "e0";
  }
  return result;
}

auto SqlQueryDialect::render_bool(bool value) const -> std::string {
  return value ? "TRUE" : "FALSE";
}

auto SqlQueryDialect::boolean_values() const -> bool {
  return true;
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
  if (not select.order_by.empty()) {
    fmt::format_to(std::back_inserter(result), " ORDER BY {}",
                   fmt::join(select.order_by, ", "));
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
    .order_by = {},
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
    .order_by = {},
    .limit = None{},
  };
}

namespace {

/// Spells the stages after an aggregation in a SQL dialect, with the semantics
/// of the target, as far as their syntax allows.
class StageRenderer {
public:
  StageRenderer(ir::OptimizeAggregation const& aggregation,
                SqlQueryDialect const& dialect)
    : aggregation_{aggregation}, dialect_{dialect} {
  }

  /// Spells a filter as a predicate.
  auto predicate(ast::expression const& expr) const -> Option<std::string> {
    TRY(auto result, render(expr));
    if (not result.predicate and not dialect_->boolean_values()) {
      return None{};
    }
    return std::move(result.text);
  }

  /// Spells the alias of the output column at `field`.
  auto column(std::span<std::string const> field) const -> Option<std::string> {
    for (auto i = std::size_t{0}; i < aggregation_->columns.size(); ++i) {
      if (std::ranges::equal(aggregation_->columns[i].output, field)) {
        return dialect_->quote_identifier(aggregation_column(i));
      }
    }
    return None{};
  }

private:
  /// Rendered text, and whether it is a predicate rather than a value.
  struct Fragment {
    std::string text;
    bool predicate = false;
  };

  auto value(ast::expression const& expr) const -> Option<std::string> {
    TRY(auto result, render(expr));
    if (result.predicate and not dialect_->boolean_values()) {
      return None{};
    }
    return std::move(result.text);
  }

  auto logical(ast::expression const& expr) const -> Option<std::string> {
    return predicate(expr);
  }

  auto literal(ast::constant::kind const& value) const -> Option<Fragment> {
    return match(
      value,
      [](caf::none_t) -> Option<Fragment> {
        return Fragment{"NULL"};
      },
      [&](bool x) -> Option<Fragment> {
        return Fragment{dialect_->render_bool(x)};
      },
      [](int64_t x) -> Option<Fragment> {
        return Fragment{fmt::to_string(x)};
      },
      [](uint64_t x) -> Option<Fragment> {
        return Fragment{fmt::to_string(x)};
      },
      [&](double x) -> Option<Fragment> {
        if (not std::isfinite(x)) {
          return None{};
        }
        return Fragment{dialect_->render_double(x)};
      },
      [&](std::string const& x) -> Option<Fragment> {
        TRY(auto text, dialect_->quote_string(x));
        return Fragment{std::move(text)};
      },
      [](auto const&) -> Option<Fragment> {
        return None{};
      });
  }

  auto field(ast::expression const& expr) const -> Option<Fragment> {
    TRY(auto path, ast::field_path::try_from(expr));
    auto names = std::vector<std::string>{};
    for (auto const& segment : path.path()) {
      if (segment.has_question_mark) {
        return None{};
      }
      names.push_back(segment.id.name);
    }
    if (names.empty()) {
      return None{};
    }
    TRY(auto alias, column(names));
    return Fragment{std::move(alias)};
  }

  auto list(ast::expression const& expr) const -> Option<std::string> {
    auto elements = std::vector<std::string>{};
    if (auto const* xs = try_as<ast::list>(expr)) {
      for (auto const& item : xs->items) {
        auto const* element = try_as<ast::expression>(item);
        if (not element) {
          return None{};
        }
        auto const* constant = try_as<ast::constant>(*element);
        if (not constant) {
          return None{};
        }
        TRY(auto x, literal(constant->value));
        elements.push_back(std::move(x.text));
      }
    } else if (auto const* constant = try_as<ast::constant>(expr)) {
      // The optimizer folds a list of constants into one constant.
      auto const* xs = try_as<tenzir::list>(constant->value);
      if (not xs) {
        return None{};
      }
      for (auto const& element : *xs) {
        auto folded = ast::constant::make(located{element, location::unknown});
        TRY(auto x, literal(folded.value));
        elements.push_back(std::move(x.text));
      }
    } else {
      return None{};
    }
    if (elements.empty()) {
      return None{};
    }
    return fmt::format("({})", fmt::join(elements, ", "));
  }

  static auto is_null(ast::expression const& expr) -> bool {
    auto const* constant = try_as<ast::constant>(expr);
    return constant and is<caf::none_t>(constant->value);
  }

  auto binary(ast::binary_expr const& x) const -> Option<Fragment> {
    using enum ast::binary_op;
    auto const spell = [&](std::string_view op, bool predicate,
                           auto const& operand) -> Option<Fragment> {
      TRY(auto left, operand(x.left));
      TRY(auto right, operand(x.right));
      return Fragment{fmt::format("({} {} {})", left, op, right), predicate};
    };
    auto const values = [&](ast::expression const& e) {
      return value(e);
    };
    auto const predicates = [&](ast::expression const& e) {
      return logical(e);
    };
    switch (x.op) {
      case eq:
      case neq: {
        auto const negated = x.op == neq;
        for (auto const* side : {&x.left, &x.right}) {
          if (is_null(*side)) {
            TRY(auto operand, value(side == &x.left ? x.right : x.left));
            return Fragment{fmt::format("({} IS {}NULL)", operand,
                                        negated ? "NOT " : ""),
                            true};
          }
        }
        return spell(negated ? "<>" : "=", true, values);
      }
      case lt:
        return spell("<", true, values);
      case leq:
        return spell("<=", true, values);
      case gt:
        return spell(">", true, values);
      case geq:
        return spell(">=", true, values);
      case add:
        return spell("+", false, values);
      case sub:
        return spell("-", false, values);
      case mul:
        return spell("*", false, values);
      case div:
        return spell("/", false, values);
      case and_:
        return spell("AND", true, predicates);
      case or_:
        return spell("OR", true, predicates);
      case in: {
        TRY(auto left, value(x.left));
        TRY(auto right, list(x.right));
        return Fragment{fmt::format("({} IN {})", left, right), true};
      }
      case if_:
      case else_:
        return None{};
    }
    TENZIR_UNREACHABLE();
  }

  auto render(ast::expression const& expr) const -> Option<Fragment> {
    if (auto const* constant = try_as<ast::constant>(expr)) {
      return literal(constant->value);
    }
    if (auto const* x = try_as<ast::binary_expr>(expr)) {
      return binary(*x);
    }
    if (auto const* x = try_as<ast::unary_expr>(expr)) {
      if (x->op == ast::unary_op::not_) {
        TRY(auto operand, logical(x->expr));
        return Fragment{fmt::format("(NOT {})", operand), true};
      }
      if (x->op == ast::unary_op::neg) {
        TRY(auto operand, value(x->expr));
        return Fragment{fmt::format("(-{})", operand)};
      }
      return None{};
    }
    return field(expr);
  }

  Ref<ir::OptimizeAggregation const> aggregation_;
  Ref<SqlQueryDialect const> dialect_;
};

/// The clauses of the query that computes the stages of an aggregation.
struct StageClauses {
  std::vector<std::string> where;
  std::vector<std::string> order_by;
  Option<uint64_t> limit;
};

auto render_stages(ir::OptimizeAggregation const& aggregation,
                   SqlQueryDialect const& dialect) -> Option<StageClauses> {
  auto const renderer = StageRenderer{aggregation, dialect};
  auto result = StageClauses{};
  auto bounded = false;
  for (auto const& stage : aggregation.stages) {
    auto ok = match(
      stage.kind,
      [&](ir::AggregationStage::Filter const& filter) {
        // SQL filters before it sorts and limits, which only a sort that
        // keeps all of its events commutes with.
        if (bounded or result.limit) {
          return false;
        }
        auto predicate = renderer.predicate(filter.predicate);
        if (not predicate) {
          return false;
        }
        result.where.push_back(std::move(*predicate));
        return true;
      },
      [&](ir::AggregationStage::Sort const& sort) {
        if (not result.order_by.empty() or result.limit or sort.keys.empty()) {
          return false;
        }
        bounded = sort.bounded;
        for (auto const& key : sort.keys) {
          auto column = renderer.column(key.field);
          if (not column) {
            return false;
          }
          result.order_by.push_back(
            fmt::format("{} {}", *column, key.descending ? "DESC" : "ASC"));
        }
        return true;
      },
      [&](ir::AggregationStage::Limit const& limit) {
        if (result.limit or limit.count > dialect.max_limit()) {
          return false;
        }
        result.limit = limit.count;
        return true;
      });
    if (not ok) {
      return None{};
    }
  }
  return result;
}

} // namespace

auto supports_post_aggregate_stages(ir::OptimizeAggregation const& aggregation,
                                    SqlQueryDialect const& dialect) -> bool {
  return render_stages(aggregation, dialect).is_some();
}

auto make_post_aggregate_select(SqlSelect aggregate,
                                ir::OptimizeAggregation const& aggregation,
                                SqlQueryDialect const& dialect)
  -> Option<SqlSelect> {
  if (aggregation.stages.empty()) {
    return aggregate;
  }
  TRY(auto clauses, render_stages(aggregation, dialect));
  auto from = fmt::format("({}) AS {}", render_select(aggregate, dialect),
                          dialect.quote_identifier("__tenzir_aggregate"));
  return SqlSelect{
    .selection = "*",
    .from = std::move(from),
    .where = std::move(clauses.where),
    .group_by = {},
    .order_by = std::move(clauses.order_by),
    .limit = clauses.limit,
  };
}

} // namespace tenzir::pushdown
