//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/detail/flat_set.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pushdown/column_model.hpp>
#include <tenzir/pushdown/renderer.hpp>

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

/// Filter, projection, and limit pushdown into the `SELECT` of `from_mysql`.
///
/// In `table` mode, `from_mysql` describes the table to the shared translation
/// (see `tenzir/pushdown/translate.hpp`) from `information_schema.columns`,
/// spells the result in MySQL SQL, and evaluates the rest itself.
///
/// Translation reasons about what TQL sees, which is what `from_mysql` decodes
/// from the text protocol: integers as `int64` or `uint64`, `FLOAT`, `DOUBLE`,
/// and `DECIMAL` as `double`, binary blobs as `blob`, and everything else as
/// the text that MySQL sends. The column model therefore covers only the
/// columns whose stored values compare like the decoded ones:
///
/// - Integer columns other than `YEAR` and `BIT`.
/// - `DOUBLE` columns without a display precision. MySQL sends them with as
///   many digits as it takes to read them back exactly, and it never stores
///   `NaN` or infinities, so they compare like TQL without guards.
/// - Text columns in a UTF-8 character set: `utf8mb4`, `utf8mb3`, or `ascii`.
///
/// Everything else stays local:
///
/// - `FLOAT`, which MySQL sends with six significant digits: TQL sees `3.14`
///   where MySQL compares `3.1400001`.
/// - `DECIMAL`, which TQL sees as the nearest `double`, while MySQL compares it
///   exactly.
/// - Temporal types, `JSON`, `ENUM`, and `SET`, which TQL sees as text while
///   MySQL compares their values.
/// - Text in other character sets, which MySQL converts to UTF-8 only when it
///   sends it, and binary strings.
/// - Invisible columns, which `SELECT *` leaves out, so TQL never sees them.
/// - Arithmetic: MySQL divides integers as `DECIMAL`, and it fails the whole
///   query when a `BIGINT UNSIGNED` result is negative or a `DOUBLE` result
///   overflows.
/// - Parsing IP addresses, since MySQL has no address type to compare with.
/// - Regular expressions, since MySQL matches them with ICU rather than RE2.
///
/// MySQL compares text under the collation of the column, and the default
/// ones ignore case, accents, trailing spaces, or NUL bytes. A literal does not
/// change that, not even a binary one, since a column's collation takes
/// precedence over that of a literal. The renderer therefore reads every text
/// column as a binary string with `CAST(col AS BINARY)`, and spells literals
/// as binary strings, so that comparisons and string functions work on bytes,
/// as in TQL. A binary string holds the bytes of the column's own character
/// set, which is why only UTF-8 columns translate. `CHAR` columns lose their
/// padding when MySQL reads them, both for TQL and for the comparison.
///
/// Matching that ignores case lowercases with `LOWER`, which differs from TQL's
/// case folding for a few characters, such as `ß`.
namespace tenzir::plugins::mysql {

/// Wraps an identifier with backticks, escaping embedded backticks.
auto quote_identifier(std::string_view name) -> std::string;

/// A column of a table, as `information_schema.columns` describes it.
struct TableColumn {
  std::string name;
  /// The type without its parameters, such as `varchar`.
  std::string data_type;
  /// The full type, such as `varchar(20)` or `int unsigned`.
  std::string column_type;
  bool nullable = true;
  /// The character set of a text column.
  Option<std::string> charset;
  /// Whether `SELECT *` includes the column.
  bool visible = true;
};

/// Reads the columns of the table whose name is the only parameter, in order.
inline constexpr auto describe_table_sql
  = std::string_view{"SELECT column_name, data_type, column_type, is_nullable, "
                     "character_set_name, extra "
                     "FROM information_schema.columns "
                     "WHERE table_schema = DATABASE() AND table_name = ? "
                     "ORDER BY ordinal_position"};

/// Parses the rows of `describe_table_sql`, skipping rows that do not have the
/// expected shape.
auto parse_table_columns(std::span<std::vector<Option<std::string>> const> rows)
  -> std::vector<TableColumn>;

/// Maps a column to how TQL sees its values. Returns `None` if predicates on
/// the column stay local.
auto parse_column_type(TableColumn const& column)
  -> Option<pushdown::ColumnType>;

/// Describes the `columns` that pushed predicates may reference.
auto make_column_model(std::span<TableColumn const> columns)
  -> pushdown::ColumnModel;

/// Spells the pushdown IR in MySQL SQL.
///
/// Every string that the renderer produces is a binary string: text columns,
/// literals, and the results of operations on them. Comparisons, `IN`, and
/// `BETWEEN` between binary strings compare bytes, and `LEFT`, `RIGHT`,
/// `INSTR`, and `LENGTH` count bytes.
class MysqlRenderer final : public pushdown::SqlRenderer {
public:
  /// Makes a renderer that reads the text columns among `columns` as binary
  /// strings.
  explicit MysqlRenderer(std::span<TableColumn const> columns);

private:
  auto quote_identifier(std::string_view name) const -> std::string override;

  auto quote_string(std::string_view text) const -> std::string override;

  auto render_double(double x) const -> std::string override;

  auto render_column(std::span<std::string const> path) const
    -> Option<Fragment> override;

  auto render_string(std::string_view text) const -> Option<Fragment> override;

  auto
  render_call(pushdown::Call const& x, std::span<Fragment const> args) const
    -> Option<Fragment> override;

  auto render_arithmetic(pushdown::Binary const& x, Fragment const& left,
                         Fragment const& right) const
    -> Option<Fragment> override;

  /// The names of the text columns.
  detail::flat_set<std::string> texts_;
};

/// How one read of a table splits the work between MySQL and the operator.
struct TablePlan {
  /// The columns to select, or `*`.
  std::string selection = "*";
  /// Predicates that MySQL evaluates, to be joined with `AND`.
  std::vector<std::string> pushed;
  /// Predicates that the operator evaluates, in order.
  ir::OptimizeFilter local;
};

/// Plans a read of a table with `columns`, an empty list if the table is
/// unknown: pushes the predicates of `filter` that MySQL evaluates exactly like
/// TQL, and selects the columns that `projection` and the remaining predicates
/// need.
auto make_table_plan(std::span<TableColumn const> columns,
                     ir::OptimizeFilter filter,
                     Option<ir::OptimizeProjection> const& projection)
  -> TablePlan;

/// Builds the query that reads `table` according to `plan`, with `LIMIT
/// limit` if every predicate was pushed.
auto make_table_query(TablePlan const& plan, std::string_view table,
                      Option<uint64_t> limit) -> std::string;

} // namespace tenzir::plugins::mysql
