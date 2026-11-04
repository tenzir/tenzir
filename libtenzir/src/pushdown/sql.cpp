//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/sql.hpp"

#include "tenzir/detail/string.hpp"
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
#include <functional>
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

/// Spells expressions of TQL in a SQL dialect, with the semantics of the
/// target, as far as their syntax allows. Fields bind through `bind`.
class ExpressionRenderer {
public:
  using Binder
    = std::function<auto(std::span<std::string const>)->Option<std::string>>;

  ExpressionRenderer(SqlQueryDialect const& dialect, Binder bind)
    : dialect_{dialect}, bind_{std::move(bind)} {
  }

  /// Spells `expr` as a predicate.
  auto predicate(ast::expression const& expr) const -> Option<std::string> {
    TRY(auto result, render(expr));
    if (not result.predicate and not dialect_->boolean_values()) {
      return None{};
    }
    return std::move(result.text);
  }

  /// Spells `expr` as a value.
  auto value(ast::expression const& expr) const -> Option<std::string> {
    TRY(auto result, render(expr));
    if (result.predicate and not dialect_->boolean_values()) {
      return None{};
    }
    return std::move(result.text);
  }

private:
  /// Rendered text, and whether it is a predicate rather than a value.
  struct Fragment {
    std::string text;
    bool predicate = false;
  };

  auto literal(ast::constant::kind const& value) const -> Option<Fragment> {
    auto wrap = [](Option<std::string> text) -> Option<Fragment> {
      TRY(auto x, std::move(text));
      return Fragment{std::move(x)};
    };
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
        return wrap(dialect_->quote_string(x));
      },
      [&](time x) -> Option<Fragment> {
        return wrap(dialect_->render_time(x));
      },
      [&](duration x) -> Option<Fragment> {
        return wrap(dialect_->render_duration(x));
      },
      [&](ip const& x) -> Option<Fragment> {
        return wrap(dialect_->render_ip(x));
      },
      [&](subnet const& x) -> Option<Fragment> {
        return wrap(dialect_->render_subnet(x));
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
    TRY(auto text, bind_(names));
    return Fragment{std::move(text)};
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
                           bool logical) -> Option<Fragment> {
      auto operand = [&](ast::expression const& e) {
        return logical ? this->predicate(e) : value(e);
      };
      TRY(auto left, operand(x.left));
      TRY(auto right, operand(x.right));
      return Fragment{fmt::format("({} {} {})", left, op, right), predicate};
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
        return spell(negated ? "<>" : "=", true, false);
      }
      case lt:
        return spell("<", true, false);
      case leq:
        return spell("<=", true, false);
      case gt:
        return spell(">", true, false);
      case geq:
        return spell(">=", true, false);
      case add:
        return spell("+", false, false);
      case sub:
        return spell("-", false, false);
      case mul:
        return spell("*", false, false);
      case div:
        return spell("/", false, false);
      case and_:
        return spell("AND", true, true);
      case or_:
        return spell("OR", true, true);
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

  auto call(ast::function_call const& x) const -> Option<Fragment> {
    auto const& ref = x.fn.ref;
    if (not ref.resolved() or ref.pkg() != entity_pkg_std
        or ref.segments().size() != 1) {
      return None{};
    }
    auto args = std::vector<std::string>{};
    for (auto const& arg : x.args) {
      // Named arguments are assignments, which no dialect spells.
      if (is<ast::assignment>(arg)) {
        return None{};
      }
      TRY(auto rendered, value(arg));
      args.push_back(std::move(rendered));
    }
    auto const& name = ref.segments().front();
    TRY(auto text, dialect_->render_call(name, args));
    return Fragment{std::move(text), dialect_->call_is_predicate(name)};
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
        TRY(auto operand, predicate(x->expr));
        return Fragment{fmt::format("(NOT {})", operand), true};
      }
      if (x->op == ast::unary_op::neg) {
        TRY(auto operand, value(x->expr));
        return Fragment{fmt::format("(-{})", operand)};
      }
      return None{};
    }
    if (auto const* x = try_as<ast::function_call>(expr)) {
      return call(*x);
    }
    return field(expr);
  }

  Ref<SqlQueryDialect const> dialect_;
  Binder bind_;
};

/// The `SELECT` that `render_plan` assembles, stage by stage.
class PlanBuilder {
public:
  PlanBuilder(SqlRelation relation, SqlQueryDialect const& dialect)
    : dialect_{dialect},
      from_{std::move(relation.from)},
      from_alias_{std::move(relation.alias)},
      input_{std::move(relation.fields)} {
  }

  auto add(ir::QueryStage const& stage) -> bool {
    return match(stage.kind, [&](auto const& x) {
      return apply(x);
    });
  }

  auto finish() && -> SqlPlan {
    auto layout = SqlLayout{
      .fields = selection_ ? output_ : input_,
      .hidden = std::move(hidden_),
      .schema_name = std::move(schema_name_),
    };
    return SqlPlan{
      .select = current(),
      .layout = std::move(layout),
      .aliases = std::move(aliases_),
    };
  }

private:
  /// The `SELECT` of the current level.
  auto current() const -> SqlSelect {
    return SqlSelect{
      .selection = selection_ ? fmt::to_string(fmt::join(*selection_, ", "))
                              : std::string{"*"},
      .from = from_,
      .where = where_,
      .group_by = group_by_,
      .order_by = order_by_,
      .limit = limit_,
    };
  }

  /// Returns whether `name` may name something that the query introduces.
  static auto is_reserved(std::string_view name) -> bool {
    constexpr auto prefix = std::string_view{"__tenzir_"};
    return name.size() >= prefix.size()
           and detail::ascii_icase_equal(name.substr(0, prefix.size()), prefix);
  }

  /// Spells `*` for the relation of the current level, qualified with its
  /// name, which the relation gets if it has none.
  auto qualified_star() -> std::string {
    if (not from_alias_) {
      auto name = relation_alias("source");
      from_ = fmt::format("{} AS {}", from_, dialect_->quote_identifier(name));
      from_alias_ = std::move(name);
    }
    return fmt::format("{}.*", dialect_->quote_identifier(*from_alias_));
  }

  /// Introduces a column name for the query.
  auto alias(std::string_view kind) -> std::string {
    auto result = relation_alias(kind);
    aliases_.push_back(result);
    return result;
  }

  /// Introduces a name for a relation of the query. A relation may share its
  /// name with a column, so the name is no alias that may collide.
  auto relation_alias(std::string_view kind) -> std::string {
    return fmt::format("__tenzir_{}_{}", kind, names_++);
  }

  /// Spells the field at `path` of the input of the current level.
  auto bind(std::span<std::string const> path) const -> Option<std::string> {
    if (not input_) {
      // A column that a drop excluded is gone.
      if (not path.empty() and std::ranges::contains(excluded_, path.front())) {
        return None{};
      }
      // The query introduces names that start with `__tenzir_`, such as the
      // name of a relation, which a target may resolve where the table has no
      // column of that name.
      if (not path.empty() and is_reserved(path.front())) {
        return None{};
      }
      return dialect_->bind_field(path);
    }
    for (auto const& [field, column] : *input_) {
      if (std::ranges::equal(field, path)) {
        return dialect_->quote_identifier(column);
      }
    }
    return None{};
  }

  auto renderer() const -> ExpressionRenderer {
    return ExpressionRenderer{*dialect_,
                              [this](std::span<std::string const> path) {
                                return bind(path);
                              }};
  }

  /// Wraps the current level into a derived table, so that the next stage
  /// sees its result as the input of a new level. Unless `keep_order` is
  /// `false`, the order of the current level survives through hidden columns.
  auto wrap(bool keep_order = true) -> void {
    // Whether the level selects its own fields, before hidden columns join
    // its selection.
    auto const projected = selection_.has_value();
    auto order = std::vector<std::pair<std::string, bool>>{};
    if (not keep_order and not limit_) {
      order_by_.clear();
    }
    if (keep_order and not order_keys_.empty()) {
      auto keys = std::vector<std::string>{};
      for (auto const& [key, descending] : order_keys_) {
        auto column = alias("order");
        hidden_.push_back(column);
        keys.push_back(
          fmt::format("{} AS {}", key, dialect_->quote_identifier(column)));
        order.emplace_back(column, descending);
      }
      if (not selection_) {
        // Some dialects, such as MySQL, accept `*` next to other expressions
        // only when it names its relation.
        selection_ = std::vector<std::string>{qualified_star()};
      }
      selection_->append_range(std::move(keys));
      // A derived table has no order of its own, unless the order decides
      // which rows the limit keeps.
      if (not limit_) {
        order_by_.clear();
      }
    }
    auto inner = current();
    auto level = relation_alias("level");
    from_ = fmt::format("({}) AS {}", render_select(inner, *dialect_),
                        dialect_->quote_identifier(level));
    from_alias_ = std::move(level);
    if (projected) {
      input_ = std::move(output_);
    }
    if (input_) {
      excluded_.clear();
    }
    selection_ = None{};
    output_ = None{};
    where_.clear();
    group_by_.clear();
    order_by_.clear();
    order_keys_.clear();
    limit_ = None{};
    for (auto const& [column, descending] : order) {
      auto key = dialect_->quote_identifier(column);
      order_by_.push_back(
        fmt::format("{} {}", key, descending ? "DESC" : "ASC"));
      order_keys_.emplace_back(key, descending);
    }
  }

  /// Whether the current level computes its own selection, groups, or limit,
  /// after which a filter, a projection, or an aggregation needs a new level.
  auto closed() const -> bool {
    return selection_ or not group_by_.empty() or limit_;
  }

  auto apply(ir::QueryStage::Filter const& x) -> bool {
    if (closed()) {
      wrap();
    }
    auto predicate = renderer().predicate(x.predicate);
    if (not predicate) {
      return false;
    }
    where_.push_back(std::move(*predicate));
    return true;
  }

  auto apply(ir::QueryStage::Select const& x) -> bool {
    if (closed()) {
      wrap();
    }
    auto selection = std::vector<std::string>{};
    auto output = SqlFields{};
    for (auto const& field : x.fields) {
      auto value = bind(field.input);
      if (not value) {
        return false;
      }
      auto column = alias("column");
      selection.push_back(
        fmt::format("{} AS {}", *value, dialect_->quote_identifier(column)));
      output.emplace_back(field.output, std::move(column));
    }
    if (selection.empty()) {
      // A query must select something to keep its rows.
      auto column = alias("row");
      hidden_.push_back(column);
      selection.push_back(
        fmt::format("1 AS {}", dialect_->quote_identifier(column)));
    }
    selection_ = std::move(selection);
    output_ = std::move(output);
    return true;
  }

  auto apply(ir::QueryStage::Drop const& x) -> bool {
    // Dropping a column that an earlier drop excluded does nothing, apart
    // from the warning of the local operator. A dialect may reject excluding
    // a column that does not exist, so the query does not repeat it.
    if (not input_ and not excluded_.empty()
        and std::ranges::all_of(x.fields, [&](auto const& path) {
              return path.size() == 1
                     and std::ranges::contains(excluded_, path.front());
            })) {
      return true;
    }
    if (closed()) {
      wrap();
    }
    if (input_) {
      // The input lists its fields, so the query selects the others. A record
      // that contains a dropped field stays, even if it becomes empty, which
      // the layout cannot express, so such a drop stays in the pipeline.
      for (auto const& path : x.fields) {
        for (auto n = size_t{1}; n < path.size(); ++n) {
          auto const ancestor = std::span{path}.first(n);
          auto survives = std::ranges::any_of(*input_, [&](auto const& entry) {
            auto const& field = entry.first;
            auto const inside
              = field.size() > n
                and std::ranges::equal(std::span{field}.first(n), ancestor);
            auto const dropped
              = std::ranges::any_of(x.fields, [&](auto const& other) {
                  return field.size() >= other.size()
                         and std::ranges::equal(
                           std::span{field}.first(other.size()), other);
                });
            return inside and not dropped;
          });
          if (not survives) {
            return false;
          }
        }
      }
      auto selection = std::vector<std::string>{};
      auto output = SqlFields{};
      for (auto const& [field, column] : *input_) {
        auto dropped = false;
        for (auto const& path : x.fields) {
          if (path.size() <= field.size()
              and std::ranges::equal(path,
                                     std::span{field}.first(path.size()))) {
            dropped = true;
          } else if (path.size() > field.size()
                     and std::ranges::equal(
                       field, std::span{path}.first(field.size()))) {
            // The column holds a record that only loses some fields.
            return false;
          }
        }
        if (not dropped) {
          selection.push_back(dialect_->quote_identifier(column));
          output.emplace_back(field, column);
        }
      }
      if (selection.empty()) {
        auto column = alias("row");
        hidden_.push_back(column);
        selection.push_back(
          fmt::format("1 AS {}", dialect_->quote_identifier(column)));
      }
      selection_ = std::move(selection);
      output_ = std::move(output);
      return true;
    }
    auto columns = std::vector<std::string>{};
    for (auto const& path : x.fields) {
      if (path.size() != 1) {
        return false;
      }
      if (not std::ranges::contains(excluded_, path.front())) {
        columns.push_back(path.front());
      }
    }
    auto selection = dialect_->render_exclude(columns);
    if (not selection) {
      return false;
    }
    // The other columns of the table stay the fields, as they are. Later
    // stages must not read the excluded ones.
    selection_ = std::vector{std::move(*selection)};
    output_ = None{};
    excluded_.append_range(std::move(columns));
    return true;
  }

  auto apply(ir::QueryStage::Aggregate const& x) -> bool {
    // An aggregation does not depend on the order of its input, unless the
    // order decides which rows a limit keeps.
    if (closed()) {
      wrap(false);
    }
    order_by_.clear();
    order_keys_.clear();
    auto selection = std::vector<std::string>{};
    auto output = SqlFields{};
    auto group_by = std::vector<std::string>{};
    for (auto const& column : x.columns) {
      auto value = Option<std::string>{};
      if (not column.function) {
        if (not column.input) {
          return false;
        }
        value = bind(*column.input);
        if (not value) {
          return false;
        }
        group_by.push_back(*value);
      } else {
        auto arg = Option<std::string>{};
        if (column.input) {
          arg = bind(*column.input);
          if (not arg) {
            return false;
          }
        }
        value = dialect_->render_aggregate(*column.function, arg);
        if (not value) {
          return false;
        }
      }
      auto name = alias("column");
      selection.push_back(
        fmt::format("{} AS {}", *value, dialect_->quote_identifier(name)));
      output.emplace_back(column.output, std::move(name));
    }
    if (selection.empty()) {
      // A summary without columns yields one empty event, also for empty
      // input, as an aggregation without groups yields one row.
      auto count = dialect_->render_aggregate("count", None{});
      if (not count) {
        return false;
      }
      auto column = alias("row");
      hidden_.push_back(column);
      selection.push_back(
        fmt::format("{} AS {}", *count, dialect_->quote_identifier(column)));
    }
    selection_ = std::move(selection);
    output_ = std::move(output);
    group_by_ = std::move(group_by);
    schema_name_ = x.schema_name;
    return true;
  }

  auto apply(ir::QueryStage::Sort const& x) -> bool {
    if (closed()) {
      wrap();
    }
    auto keys = std::vector<std::pair<std::string, bool>>{};
    for (auto const& key : x.keys) {
      auto value = bind(key.field);
      if (not value) {
        return false;
      }
      keys.emplace_back(std::move(*value), key.descending);
    }
    if (keys.empty()) {
      return false;
    }
    // A sort after another one breaks its ties by the earlier order. A key
    // counts only once, at its first position, since SQL Server rejects
    // duplicate keys.
    keys.append_range(std::move(order_keys_));
    order_keys_.clear();
    for (auto& [key, descending] : keys) {
      auto const same = [&](auto const& other) {
        return other.first == key;
      };
      if (std::ranges::any_of(order_keys_, same)) {
        continue;
      }
      // A target may resolve keys that differ only in case to the same
      // column, which SQL Server then rejects as a duplicate, or to different
      // ones, which would need both.
      if (std::ranges::any_of(order_keys_, [&](auto const& other) {
            return detail::ascii_icase_equal(other.first, key);
          })) {
        return false;
      }
      order_keys_.emplace_back(std::move(key), descending);
    }
    order_by_.clear();
    for (auto const& [key, descending] : order_keys_) {
      order_by_.push_back(
        fmt::format("{} {}", key, descending ? "DESC" : "ASC"));
    }
    return true;
  }

  auto apply(ir::QueryStage::Limit const& x) -> bool {
    if (x.count > dialect_->max_limit()) {
      return false;
    }
    limit_ = limit_ ? std::min(*limit_, x.count) : x.count;
    return true;
  }

  Ref<SqlQueryDialect const> dialect_;
  /// The relation of the current level.
  std::string from_;
  /// The name of the relation of the current level, if it has one.
  Option<std::string> from_alias_;
  /// The fields of the input of the current level, or `None` if they bind
  /// through the dialect.
  Option<SqlFields> input_;
  /// The selection of the current level, or `None` for `*`.
  Option<std::vector<std::string>> selection_;
  /// The fields that the selection produces, or `None` if they are the
  /// columns that it keeps, as for an exclusion of columns.
  Option<SqlFields> output_;
  std::vector<std::string> where_;
  std::vector<std::string> group_by_;
  std::vector<std::string> order_by_;
  /// The keys of the order of the current level, in its input.
  std::vector<std::pair<std::string, bool>> order_keys_;
  Option<uint64_t> limit_;
  /// The columns of an unlisted input that a drop excluded.
  std::vector<std::string> excluded_;
  std::vector<std::string> hidden_;
  std::vector<std::string> aliases_;
  /// The number of names that the query introduced, including relations.
  size_t names_ = 0;
  Option<std::string> schema_name_;
};

/// The result columns of `aggregation`, as the fields of a relation.
auto aggregate_fields(ir::OptimizeAggregation const& aggregation) -> SqlFields {
  auto result = SqlFields{};
  for (auto i = std::size_t{0}; i < aggregation.columns.size(); ++i) {
    result.emplace_back(aggregation.columns[i].output, aggregation_column(i));
  }
  return result;
}

} // namespace

auto SqlQueryDialect::bind_field(std::span<std::string const> path) const
  -> Option<std::string> {
  if (path.size() != 1) {
    return None{};
  }
  return quote_identifier(path.front());
}

auto SqlQueryDialect::render_time(time value) const -> Option<std::string> {
  TENZIR_UNUSED(value);
  return None{};
}

auto SqlQueryDialect::render_duration(duration value) const
  -> Option<std::string> {
  TENZIR_UNUSED(value);
  return None{};
}

auto SqlQueryDialect::render_ip(ip value) const -> Option<std::string> {
  TENZIR_UNUSED(value);
  return None{};
}

auto SqlQueryDialect::render_subnet(subnet value) const -> Option<std::string> {
  TENZIR_UNUSED(value);
  return None{};
}

auto SqlQueryDialect::render_call(std::string_view name,
                                  std::span<std::string const> args) const
  -> Option<std::string> {
  TENZIR_UNUSED(name, args);
  return None{};
}

auto SqlQueryDialect::call_is_predicate(std::string_view name) const -> bool {
  TENZIR_UNUSED(name);
  return false;
}

auto SqlQueryDialect::render_aggregate(std::string_view name,
                                       Option<std::string> const& arg) const
  -> Option<std::string> {
  if (name == "count") {
    return fmt::format("COUNT({})", arg ? *arg : std::string{"*"});
  }
  if (not arg) {
    return None{};
  }
  if (name == "min") {
    return fmt::format("MIN({})", *arg);
  }
  if (name == "max") {
    return fmt::format("MAX({})", *arg);
  }
  return None{};
}

auto SqlQueryDialect::render_exclude(std::span<std::string const> columns) const
  -> Option<std::string> {
  TENZIR_UNUSED(columns);
  return None{};
}

auto render_plan(SqlRelation relation, std::span<ir::QueryStage const> plan,
                 SqlQueryDialect const& dialect) -> Option<SqlPlan> {
  auto builder = PlanBuilder{std::move(relation), dialect};
  for (auto const& stage : plan) {
    if (not builder.add(stage)) {
      return None{};
    }
  }
  return std::move(builder).finish();
}

auto supports_post_aggregate_stages(ir::OptimizeAggregation const& aggregation,
                                    SqlQueryDialect const& dialect) -> bool {
  auto relation = SqlRelation{
    .from = "t",
    .fields = aggregate_fields(aggregation),
    .alias = None{},
  };
  return render_plan(std::move(relation), aggregation.stages, dialect).is_some();
}

auto make_post_aggregate_select(SqlSelect aggregate,
                                ir::OptimizeAggregation const& aggregation,
                                SqlQueryDialect const& dialect)
  -> Option<SqlSelect> {
  if (aggregation.stages.empty()) {
    return aggregate;
  }
  auto relation = SqlRelation{
    .from = fmt::format("({}) AS {}", render_select(aggregate, dialect),
                        dialect.quote_identifier("__tenzir_aggregate")),
    .fields = aggregate_fields(aggregation),
    .alias = "__tenzir_aggregate",
  };
  TRY(auto plan, render_plan(std::move(relation), aggregation.stages, dialect));
  return std::move(plan.select);
}

} // namespace tenzir::pushdown
