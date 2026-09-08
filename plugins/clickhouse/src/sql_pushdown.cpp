//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/sql_pushdown.hpp"

#include "clickhouse/arguments.hpp"
#include "clickhouse/transformers.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <concepts>
#include <limits>
#include <ranges>
#include <set>

namespace tenzir::plugins::clickhouse {

namespace {

using namespace std::string_view_literals;

/// Strips `Nullable(...)` and `LowCardinality(...)` wrappers. Sets `nullable`
/// if a `Nullable` wrapper was present.
auto unwrap_column_type(std::string_view type, bool& nullable)
  -> std::string_view {
  while (true) {
    if (auto inner = unwrap_clickhouse_type_call(type, "Nullable")) {
      nullable = true;
      type = *inner;
      continue;
    }
    if (auto inner = unwrap_clickhouse_type_call(type, "LowCardinality")) {
      type = *inner;
      continue;
    }
    return type;
  }
}

/// Maps a bare ClickHouse type name to the kind that compares against TQL
/// constants with identical semantics. Types that TQL decodes to a different
/// representation (e.g. `FixedString` padding, `Enum` names, `Decimal`
/// scaling) or that ClickHouse compares differently are left out.
auto kind_of_type(std::string_view type) -> Option<SqlKind> {
  constexpr auto integers = std::array{
    "Int8"sv,  "Int16"sv,  "Int32"sv,  "Int64"sv,
    "UInt8"sv, "UInt16"sv, "UInt32"sv, "UInt64"sv,
  };
  if (type == "Bool") {
    return SqlKind::boolean;
  }
  if (type == "String") {
    return SqlKind::string;
  }
  if (std::ranges::contains(integers, type)) {
    return SqlKind::integer;
  }
  if (type == "Float32" or type == "Float64") {
    return SqlKind::floating;
  }
  return None{};
}

/// The smallest integer magnitude that a `double` cannot represent exactly.
constexpr auto exact_double_bound = 9007199254740992.0;

auto is_number(SqlKind kind) -> bool {
  return kind == SqlKind::integer or kind == SqlKind::floating;
}

/// Returns whether `x` converts to `double` and back without loss.
template <class T>
  requires std::integral<T>
auto is_exact_in_double(T x) -> bool {
  auto d = static_cast<double>(x);
  // Guard the conversion back: `double` rounds the extremes of the range up
  // to a power of two that `T` cannot hold.
  if (d >= static_cast<double>(std::numeric_limits<T>::max())
      or d < static_cast<double>(std::numeric_limits<T>::min())) {
    return false;
  }
  return static_cast<T>(d) == x;
}

/// Returns whether comparing a column of `kind` with `literal` yields the same
/// result in TQL and ClickHouse.
///
/// Same-kind comparisons always do. Mixed comparisons differ in how they are
/// evaluated: TQL converts the integer side to `double` and compares doubles,
/// while ClickHouse compares the two numbers exactly. When the integer side is
/// the literal, both agree as long as the literal converts exactly. When the
/// integer side is the column, its values are what gets rounded, and both
/// agree as long as the literal stays below `exact_double_bound`: a column
/// value at or beyond it then rounds to a `double` on the same side of the
/// literal.
auto literal_fits(SqlKind kind, ast::constant const& literal) -> bool {
  return match(
    literal.value,
    [&](bool) {
      return kind == SqlKind::boolean;
    },
    [&](std::string const&) {
      return kind == SqlKind::string;
    },
    [&](int64_t x) {
      return kind == SqlKind::integer
             or (kind == SqlKind::floating and is_exact_in_double(x));
    },
    [&](uint64_t x) {
      return kind == SqlKind::integer
             or (kind == SqlKind::floating and is_exact_in_double(x));
    },
    [&](double x) {
      // TQL has no syntax for non-finite literals; reject them defensively.
      if (not std::isfinite(x)) {
        return false;
      }
      return kind == SqlKind::floating
             or (kind == SqlKind::integer
                 and std::fabs(x) < exact_double_bound);
    },
    [](auto const&) {
      return false;
    });
}

/// Returns `expr` as a constant if it is one, folding a leading minus on a
/// numeric literal, which the parser represents as `neg(constant)`.
auto as_literal(ast::expression const& expr) -> Option<ast::constant> {
  if (auto const* constant = try_as<ast::constant>(expr)) {
    return *constant;
  }
  auto const* unary = try_as<ast::unary_expr>(expr);
  if (not unary or unary->op != ast::unary_op::neg) {
    return None{};
  }
  auto const* inner = try_as<ast::constant>(unary->expr);
  if (not inner) {
    return None{};
  }
  return match(
    inner->value,
    [&](int64_t x) -> Option<ast::constant> {
      return ast::constant{-x, unary->location};
    },
    [&](double x) -> Option<ast::constant> {
      return ast::constant{-x, unary->location};
    },
    [](auto const&) -> Option<ast::constant> {
      // Negating a `uint64` literal beyond the `int64` range or a non-number
      // is not a plain literal.
      return None{};
    });
}

/// Renders a constant as a SQL literal. Requires `literal_fits` to have
/// accepted the constant.
auto render_constant(ast::constant const& constant) -> std::string {
  return match(
    constant.value,
    [](bool x) -> std::string {
      return x ? "true" : "false";
    },
    [](int64_t x) -> std::string {
      return fmt::to_string(x);
    },
    [](uint64_t x) -> std::string {
      return fmt::to_string(x);
    },
    [](double x) -> std::string {
      // `{}` picks the shortest representation that round-trips.
      return fmt::format("{}", x);
    },
    [](std::string const& x) -> std::string {
      return quote_sql_string(x);
    },
    [](auto const&) -> std::string {
      TENZIR_UNREACHABLE();
    });
}

struct ResolvedColumn {
  std::string sql;
  SqlColumn column;
};

/// Resolves a field path expression to a column of `schema`.
auto resolve_column(ast::expression const& expr, SqlSchema const& schema)
  -> Option<ResolvedColumn> {
  auto path = ast::field_path::try_from(expr);
  if (not path or path->path().empty()) {
    return None{};
  }
  auto segments = std::vector<std::string>{};
  auto sql = std::string{};
  for (auto const& segment : path->path()) {
    segments.push_back(segment.id.name);
    if (not sql.empty()) {
      sql += '.';
    }
    sql += quote_sql_identifier(segment.id.name);
  }
  auto column = schema.find(segments);
  if (not column) {
    return None{};
  }
  return ResolvedColumn{.sql = std::move(sql), .column = *column};
}

auto render_comparison(ast::binary_op op) -> std::string_view {
  switch (op) {
    case ast::binary_op::eq:
      return "=";
    case ast::binary_op::neq:
      return "!=";
    case ast::binary_op::lt:
      return "<";
    case ast::binary_op::leq:
      return "<=";
    case ast::binary_op::gt:
      return ">";
    case ast::binary_op::geq:
      return ">=";
    default:
      TENZIR_UNREACHABLE();
  }
}

/// Mirrors `op` so that the column ends up on the left-hand side.
auto flip_comparison(ast::binary_op op) -> ast::binary_op {
  switch (op) {
    case ast::binary_op::lt:
      return ast::binary_op::gt;
    case ast::binary_op::leq:
      return ast::binary_op::geq;
    case ast::binary_op::gt:
      return ast::binary_op::lt;
    case ast::binary_op::geq:
      return ast::binary_op::leq;
    default:
      return op;
  }
}

auto translate_comparison(ast::binary_expr const& expr, SqlSchema const& schema)
  -> Option<std::string> {
  // Exactly one side must be a column and the other a constant.
  auto op = expr.op;
  auto const* column_expr = &expr.right;
  auto constant = as_literal(expr.right);
  if (not constant) {
    constant = as_literal(expr.left);
    if (not constant) {
      return None{};
    }
    op = flip_comparison(op);
  } else {
    column_expr = &expr.left;
  }
  auto column = resolve_column(*column_expr, schema);
  if (not column) {
    return None{};
  }
  // `x == null` and `x != null` are null checks in TQL.
  if (is<caf::none_t>(constant->value)) {
    switch (op) {
      case ast::binary_op::eq:
        return fmt::format("{} IS NULL", column->sql);
      case ast::binary_op::neq:
        return fmt::format("{} IS NOT NULL", column->sql);
      default:
        return None{};
    }
  }
  if (not literal_fits(column->column.kind, *constant)) {
    return None{};
  }
  auto literal = render_constant(*constant);
  switch (op) {
    case ast::binary_op::eq:
      if (column->column.nullable) {
        return fmt::format("({0} IS NOT NULL AND {0} = {1})", column->sql,
                           literal);
      }
      return fmt::format("{} = {}", column->sql, literal);
    case ast::binary_op::neq:
      if (column->column.nullable) {
        return fmt::format("({0} IS NULL OR {0} != {1})", column->sql, literal);
      }
      return fmt::format("{} != {}", column->sql, literal);
    case ast::binary_op::lt:
    case ast::binary_op::leq:
    case ast::binary_op::gt:
    case ast::binary_op::geq:
      // Ordering yields `null` for `null` operands in both TQL and SQL, so no
      // guard is needed. It is only pushed for numbers, where both sides agree
      // on the order, including IEEE 754 semantics for `nan`: neither TQL nor
      // ClickHouse's comparison operators order it.
      if (not is_number(column->column.kind)) {
        return None{};
      }
      return fmt::format("{} {} {}", column->sql, render_comparison(op),
                         literal);
    default:
      TENZIR_UNREACHABLE();
  }
}

auto translate_in(ast::binary_expr const& expr, SqlSchema const& schema)
  -> Option<std::string> {
  auto const* list = try_as<ast::list>(expr.right);
  if (not list or list->items.empty()) {
    return None{};
  }
  auto column = resolve_column(expr.left, schema);
  if (not column) {
    return None{};
  }
  auto literals = std::vector<std::string>{};
  auto element_type = Option<size_t>{};
  for (auto const& item : list->items) {
    auto const* item_expr = try_as<ast::expression>(item);
    if (not item_expr) {
      return None{};
    }
    auto constant = as_literal(*item_expr);
    // A `null` in the list makes TQL and SQL disagree for `null` rows.
    if (not constant or is<caf::none_t>(constant->value)) {
      return None{};
    }
    // A TQL list has a single element type, fixed by its first element; a
    // later element of another type becomes `null` and never matches, even
    // between `int64` and `double` or `int64` and `uint64`. ClickHouse would
    // match it, so such lists stay local.
    auto type_index
      = variant_traits<ast::constant::kind>::index(constant->value);
    if (element_type and *element_type != type_index) {
      return None{};
    }
    element_type = type_index;
    // ClickHouse converts set elements to the column type and drops elements
    // that do not convert exactly, so `int_col IN (1.5)` never matches, just
    // like `x in [1.5]` in TQL.
    if (not literal_fits(column->column.kind, *constant)) {
      return None{};
    }
    literals.push_back(render_constant(*constant));
  }
  auto set = fmt::format("({})", fmt::join(literals, ", "));
  if (column->column.nullable) {
    return fmt::format("({0} IS NOT NULL AND {0} IN {1})", column->sql, set);
  }
  return fmt::format("{} IN {}", column->sql, set);
}

auto translate_binary(ast::binary_expr const& expr, SqlSchema const& schema)
  -> Option<std::string> {
  switch (expr.op) {
    case ast::binary_op::and_:
    case ast::binary_op::or_: {
      auto left = translate_predicate(expr.left, schema);
      if (not left) {
        return None{};
      }
      auto right = translate_predicate(expr.right, schema);
      if (not right) {
        return None{};
      }
      auto const* keyword = expr.op == ast::binary_op::and_ ? "AND" : "OR";
      return fmt::format("({} {} {})", *left, keyword, *right);
    }
    case ast::binary_op::eq:
    case ast::binary_op::neq:
    case ast::binary_op::lt:
    case ast::binary_op::leq:
    case ast::binary_op::gt:
    case ast::binary_op::geq:
      return translate_comparison(expr, schema);
    case ast::binary_op::in:
      return translate_in(expr, schema);
    default:
      return None{};
  }
}

/// Appends the conjuncts of `expr` to `out`, splitting nested `and`s.
///
/// Splitting lets a translatable conjunct go into SQL while an earlier one
/// stays local, so the local one then sees only the rows that survived the
/// pushed one. This is safe because a conjunction selects the same rows in any
/// order and `where` predicates are pure. It only changes how often a local
/// predicate is evaluated, and with it the number of warnings it emits for
/// rows that the result never contains. TQL's `and` already evaluates its right
/// side only on rows the left side did not decide, and `export` splits a
/// conjunction the same way.
auto collect_conjuncts(ast::expression expr, ir::OptimizeFilter& out) -> void {
  if (auto* binary = try_as<ast::binary_expr>(expr);
      binary and binary->op == ast::binary_op::and_) {
    collect_conjuncts(std::move(binary->left), out);
    collect_conjuncts(std::move(binary->right), out);
    return;
  }
  out.push_back(std::move(expr));
}

} // namespace

auto SqlSchema::add_column(std::string_view name, std::string_view type,
                           bool generated) -> void {
  columns_.emplace_back(name);
  if (generated) {
    generated_.emplace(name);
    return;
  }
  auto normalized = remove_non_significant_whitespace(type);
  auto path = std::vector<std::string>{std::string{name}};
  add_path(path, normalized);
}

auto SqlSchema::is_generated(std::string_view name) const -> bool {
  return generated_.contains(name);
}

auto SqlSchema::add_path(std::vector<std::string>& path, std::string_view type)
  -> void {
  auto nullable = false;
  auto bare = unwrap_column_type(type, nullable);
  if (auto kind = kind_of_type(bare)) {
    columns_by_path_.emplace(path,
                             SqlColumn{.kind = *kind, .nullable = nullable});
    return;
  }
  auto elements = unwrap_clickhouse_type_call(bare, "Tuple");
  if (not elements) {
    return;
  }
  for (auto element : split_top_level_clickhouse_type_arguments(*elements)) {
    auto split = find_top_level_clickhouse_type_space(element);
    // Unnamed tuple elements are only addressable by index in SQL.
    if (split == std::string_view::npos) {
      continue;
    }
    path.push_back(
      unquote_identifier_component(detail::trim(element.substr(0, split))));
    add_path(path, detail::trim(element.substr(split + 1)));
    path.pop_back();
  }
}

auto SqlSchema::columns() const -> std::span<const std::string> {
  return columns_;
}

auto SqlSchema::find(std::span<const std::string> path) const
  -> Option<SqlColumn> {
  auto it
    = columns_by_path_.find(std::vector<std::string>{path.begin(), path.end()});
  if (it == columns_by_path_.end()) {
    return None{};
  }
  return it->second;
}

auto split_filter_for_sql(ir::OptimizeFilter filter, SqlSchema const& schema)
  -> SqlFilterSplit {
  auto conjuncts = ir::OptimizeFilter{};
  for (auto& expr : filter) {
    collect_conjuncts(std::move(expr), conjuncts);
  }
  auto result = SqlFilterSplit{};
  for (auto& expr : conjuncts) {
    if (auto sql = translate_predicate(expr, schema)) {
      result.pushed.push_back(std::move(*sql));
    } else {
      result.remaining.push_back(std::move(expr));
    }
  }
  return result;
}

auto translate_predicate(ast::expression const& expr, SqlSchema const& schema)
  -> Option<std::string> {
  return match(
    expr,
    [&](ast::binary_expr const& x) -> Option<std::string> {
      return translate_binary(x, schema);
    },
    [&](ast::unary_expr const& x) -> Option<std::string> {
      if (x.op != ast::unary_op::not_) {
        return None{};
      }
      auto inner = translate_predicate(x.expr, schema);
      if (not inner) {
        return None{};
      }
      return fmt::format("NOT {}", *inner);
    },
    [&](ast::root_field const&) -> Option<std::string> {
      // A bare boolean column as predicate.
      auto column = resolve_column(expr, schema);
      if (not column or column->column.kind != SqlKind::boolean) {
        return None{};
      }
      return column->sql;
    },
    [&](ast::field_access const&) -> Option<std::string> {
      auto column = resolve_column(expr, schema);
      if (not column or column->column.kind != SqlKind::boolean) {
        return None{};
      }
      return column->sql;
    },
    [](auto const&) -> Option<std::string> {
      return None{};
    });
}

auto make_select_query(std::string_view table, SqlSchema const* schema,
                       Option<ir::OptimizeProjection> const& projection,
                       std::span<const std::string> where,
                       Option<uint64_t> limit) -> std::string {
  auto columns = std::string{"*"};
  if (schema and projection) {
    auto wanted = std::set<std::string>{};
    for (auto const& path : *projection) {
      auto segments = path.path();
      if (not segments.empty()) {
        wanted.insert(segments.front().id.name);
      }
    }
    // Whether `SELECT *` contains a generated column depends on session
    // settings, so a projection that names one keeps `*` to match either way.
    auto names_generated = std::ranges::any_of(wanted, [&](auto const& name) {
      return schema->is_generated(name);
    });
    auto selected = std::vector<std::string>{};
    for (auto const& column : schema->columns()) {
      if (wanted.contains(column)) {
        selected.push_back(quote_sql_identifier(column));
      }
    }
    if (not names_generated and not selected.empty()) {
      columns = fmt::format("{}", fmt::join(selected, ", "));
    }
  }
  auto query = fmt::format("SELECT {} FROM {}", columns, table);
  if (not where.empty()) {
    fmt::format_to(std::back_inserter(query), " WHERE {}",
                   fmt::join(where, " AND "));
  }
  if (limit) {
    fmt::format_to(std::back_inserter(query), " LIMIT {}", *limit);
  }
  return query;
}

auto quote_sql_identifier(std::string_view name) -> std::string {
  auto result = std::string{"`"};
  for (auto c : name) {
    if (c == '`' or c == '\\') {
      result += '\\';
    }
    result += c;
  }
  result += '`';
  return result;
}

auto quote_sql_string(std::string_view text) -> std::string {
  auto result = std::string{"'"};
  for (auto c : text) {
    auto byte = static_cast<unsigned char>(c);
    if (c == '\'' or c == '\\') {
      result += '\\';
      result += c;
    } else if (byte < 0x20 or byte == 0x7f) {
      fmt::format_to(std::back_inserter(result), "\\x{:02X}", byte);
    } else {
      result += c;
    }
  }
  result += '\'';
  return result;
}

} // namespace tenzir::plugins::clickhouse
