//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/builtins/from_mysql/pushdown.hpp"

#include <tenzir/detail/assert.hpp>
#include <tenzir/detail/string.hpp>
#include <tenzir/pushdown/aggregation.hpp>
#include <tenzir/pushdown/sql.hpp>
#include <tenzir/pushdown/translate.hpp>
#include <tenzir/try.hpp>
#include <tenzir/unicode.hpp>

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <iterator>
#include <utility>

namespace tenzir::plugins::mysql {

namespace {

auto lowercase(std::string_view text) -> std::string {
  auto result = std::string{text};
  std::ranges::transform(result, result.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return result;
}

/// Spells the parts of a `SELECT` that SQL dialects disagree on in MySQL SQL.
class MysqlQueryDialect final : public pushdown::SqlQueryDialect {
public:
  auto quote_identifier(std::string_view name) const -> std::string override {
    return mysql::quote_identifier(name);
  }
};

/// Spells aggregations over a table in MySQL SQL.
class MysqlAggregateDialect final : public pushdown::SqlAggregateDialect {
public:
  explicit MysqlAggregateDialect(std::span<TableColumn const> columns)
    : columns_{columns}, renderer_{columns} {
  }

private:
  auto alias_collides(std::string_view alias) const -> bool override {
    // MySQL compares column names case-insensitively.
    return std::ranges::any_of(columns_, [&](TableColumn const& column) {
      return detail::ascii_icase_equal(column.name, alias);
    });
  }

  auto render_value(pushdown::Expr const& expr) const
    -> Option<std::string> override {
    return renderer_.render(expr);
  }

  auto
  render_key(std::span<std::string const> path,
             pushdown::FieldValue const& field, std::string const& value) const
    -> Option<std::string> override {
    auto const& type = field.column.type;
    if (is<pushdown::StringType>(type)) {
      // The query groups by the binary string, which may arrive as a blob,
      // and selects the text as stored.
      TENZIR_ASSERT(path.size() == 1);
      return fmt::format("ANY_VALUE({})",
                         mysql::quote_identifier(path.front()));
    }
    if (is<pushdown::IntType>(type)) {
      return value;
    }
    return None{};
  }

  auto render_count(Option<std::string> const& value) const
    -> std::string override {
    return fmt::format("CAST(COUNT({}) AS SIGNED)",
                       value ? *value : std::string{"*"});
  }

  auto render_extremum(pushdown::Extremum extremum,
                       pushdown::FieldValue const& field,
                       std::string const& value) const
    -> Option<std::string> override {
    if (not is<pushdown::IntType>(field.column.type)) {
      return None{};
    }
    return fmt::format(
      "{}({})", extremum == pushdown::Extremum::min ? "MIN" : "MAX", value);
  }

  std::span<TableColumn const> columns_;
  MysqlRenderer renderer_;
};

} // namespace

auto quote_identifier(std::string_view name) -> std::string {
  auto result = std::string{"`"};
  for (auto c : name) {
    if (c == '`') {
      result += "``";
    } else {
      result += c;
    }
  }
  result += '`';
  return result;
}

auto parse_table_columns(std::span<std::vector<Option<std::string>> const> rows)
  -> std::vector<TableColumn> {
  auto result = std::vector<TableColumn>{};
  for (auto const& row : rows) {
    if (row.size() != 6 or not row[0] or not row[1] or not row[2]
        or not row[3]) {
      continue;
    }
    result.push_back(TableColumn{
      .name = *row[0],
      .data_type = lowercase(*row[1]),
      .column_type = lowercase(*row[2]),
      .nullable = *row[3] != "NO",
      .charset = row[4] ? Option<std::string>{lowercase(*row[4])} : None{},
      .visible
      = not row[5] or lowercase(*row[5]).find("invisible") == std::string::npos,
    });
  }
  return result;
}

auto parse_column_type(TableColumn const& column)
  -> Option<pushdown::ColumnType> {
  constexpr auto integers
    = std::array<std::pair<std::string_view, uint8_t>, 5>{{
      {"tinyint", 8},
      {"smallint", 16},
      {"mediumint", 24},
      {"int", 32},
      {"bigint", 64},
    }};
  for (auto const& [name, bits] : integers) {
    if (column.data_type == name) {
      return pushdown::IntType{
        .bits = bits,
        .is_signed = column.column_type.find("unsigned") == std::string::npos,
      };
    }
  }
  // A display precision, as in `DOUBLE(10,2)`, rounds the text that TQL sees.
  if (column.data_type == "double"
      and (column.column_type == "double"
           or column.column_type == "double unsigned")) {
    return pushdown::FloatType{};
  }
  constexpr auto texts = std::array<std::string_view, 6>{
    "char", "varchar", "tinytext", "text", "mediumtext", "longtext",
  };
  // `utf8` is the name of `utf8mb3` before MySQL 8.0.30. ASCII is a subset of
  // UTF-8.
  constexpr auto charsets = std::array<std::string_view, 4>{
    "utf8mb4",
    "utf8mb3",
    "utf8",
    "ascii",
  };
  if (std::ranges::contains(texts, column.data_type) and column.charset
      and std::ranges::contains(charsets, *column.charset)) {
    return pushdown::StringType{};
  }
  return None{};
}

auto make_column_model(std::span<TableColumn const> columns)
  -> pushdown::ColumnModel {
  auto result = pushdown::ColumnModel{};
  for (auto const& column : columns) {
    if (not column.visible) {
      continue;
    }
    if (auto type = parse_column_type(column)) {
      result.add({column.name},
                 {.type = std::move(*type), .nullable = column.nullable});
    }
  }
  return result;
}

MysqlRenderer::MysqlRenderer(std::span<TableColumn const> columns) {
  for (auto const& column : columns) {
    auto type = parse_column_type(column);
    if (type and is<pushdown::StringType>(*type)) {
      texts_.insert(column.name);
    }
  }
}

auto MysqlRenderer::quote_identifier(std::string_view name) const
  -> std::string {
  return mysql::quote_identifier(name);
}

auto MysqlRenderer::quote_string(std::string_view text) const -> std::string {
  // A `_binary` literal reads like the text, but MySQL interprets backslashes
  // in it unless the session sets `NO_BACKSLASH_ESCAPES`. A hex literal is a
  // binary string, too, and means the same in every mode.
  if (text.find_first_of(std::string_view{"\\\0", 2}) != std::string::npos) {
    auto result = std::string{"X'"};
    for (auto c : text) {
      fmt::format_to(std::back_inserter(result), "{:02X}",
                     static_cast<unsigned char>(c));
    }
    result += '\'';
    return result;
  }
  auto result = std::string{"_binary'"};
  for (auto c : text) {
    if (c == '\'') {
      result += "''";
    } else {
      result += c;
    }
  }
  result += '\'';
  return result;
}

auto MysqlRenderer::render_double(double x) const -> std::string {
  // MySQL reads `0.1` as a `DECIMAL`, which it compares with an integer
  // column exactly. An exponent makes it a `DOUBLE`.
  auto result = fmt::format("{}", x);
  if (result.find('e') == std::string::npos) {
    result += "e0";
  }
  return result;
}

auto MysqlRenderer::render_column(std::span<std::string const> path) const
  -> Option<Fragment> {
  TRY(auto column, SqlRenderer::render_column(path));
  if (path.size() == 1 and texts_.contains(path.front())) {
    // Compares bytes instead of applying the column's collation.
    return sql_cast(column, "BINARY");
  }
  return column;
}

auto MysqlRenderer::render_string(std::string_view text) const
  -> Option<Fragment> {
  // A text column holds valid UTF-8 only, and `fold_case` reads the literal as
  // UTF-8.
  if (not unicode::is_valid_utf8(text)) {
    return None{};
  }
  return SqlRenderer::render_string(text);
}

auto MysqlRenderer::render_call(pushdown::Call const& x,
                                std::span<Fragment const> args) const
  -> Option<Fragment> {
  // The arguments are binary strings, so the functions count and compare
  // bytes.
  auto const op = x.op;
  switch (op) {
    case pushdown::Operation::starts_with:
    case pushdown::Operation::ends_with: {
      TENZIR_ASSERT(args.size() == 2);
      auto length = sql_call("LENGTH", std::array{args[1]});
      auto affix
        = sql_call(op == pushdown::Operation::starts_with ? "LEFT" : "RIGHT",
                   std::array{args[0], std::move(length)});
      return sql_binary("=", affix, args[1], Fragment::Kind::predicate);
    }
    case pushdown::Operation::contains:
      // `INSTR` finds the empty needle at position 1.
      TENZIR_ASSERT(args.size() == 2);
      return sql_binary(">", sql_call("INSTR", args), sql_atom("0"),
                        Fragment::Kind::predicate);
    case pushdown::Operation::length_bytes:
      // `CHAR_LENGTH` counts characters.
      TENZIR_ASSERT(args.size() == 1);
      return sql_call("LENGTH", args);
    case pushdown::Operation::parse_ip:
      // MySQL has no address type to compare the result with.
      return None{};
    case pushdown::Operation::fold_case: {
      // `LOWER` leaves binary strings alone, so the argument is read as UTF-8
      // first. MySQL lowercases per code point, so `ß` stays `ß` where TQL
      // folds it to `ss`.
      TENZIR_ASSERT(args.size() == 1);
      auto text = sql_cast(args[0], "CHAR CHARACTER SET utf8mb4");
      auto lower = sql_call("LOWER", std::array{std::move(text)});
      return sql_cast(lower, "BINARY");
    }
    case pushdown::Operation::match_regex:
      // MySQL matches with ICU, whose semantics differ from RE2, for example
      // in where `$` matches and in case sensitivity under a collation.
      return None{};
  }
  TENZIR_UNREACHABLE();
}

auto MysqlRenderer::render_arithmetic(pushdown::Binary const&, Fragment const&,
                                      Fragment const&) const
  -> Option<Fragment> {
  // MySQL divides integers as `DECIMAL`, and fails the query when a
  // `BIGINT UNSIGNED` result is negative or a `DOUBLE` result overflows.
  return None{};
}

auto make_table_plan(std::span<TableColumn const> columns,
                     ir::OptimizeFilter filter,
                     Option<ir::OptimizeProjection> const& projection,
                     registry const& reg) -> TablePlan {
  auto table = std::vector<pushdown::SqlColumn>{};
  for (auto const& column : columns) {
    table.push_back({.name = column.name, .visible = column.visible});
  }
  return TablePlan{
    pushdown::make_table_plan(table, make_column_model(columns),
                              MysqlRenderer{columns}, MysqlQueryDialect{},
                              std::move(filter), projection, reg),
    {columns.begin(), columns.end()},
  };
}

auto supports_aggregate_functions(ir::OptimizeAggregation const& aggregation)
  -> bool {
  return pushdown::supports_count_min_max(aggregation);
}

auto make_aggregate_query(ir::OptimizeAggregation const& aggregation,
                          std::span<TableColumn const> columns,
                          std::string_view table,
                          std::span<std::string const> where)
  -> Option<std::string> {
  if (columns.empty()) {
    return None{};
  }
  TRY(auto plan, pushdown::classify(aggregation));
  TRY(auto select,
      pushdown::make_aggregate_select(
        plan, make_column_model(columns), MysqlQueryDialect{},
        MysqlAggregateDialect{columns}, quote_identifier(table), where));
  return pushdown::render_select(select, MysqlQueryDialect{});
}

auto make_table_query(TablePlan const& plan, std::string_view table,
                      Option<uint64_t> limit) -> std::string {
  auto const dialect = MysqlQueryDialect{};
  return pushdown::render_select(
    pushdown::make_table_select(plan, quote_identifier(table), limit, dialect),
    dialect);
}

} // namespace tenzir::plugins::mysql
