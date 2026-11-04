//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <tenzir/ir.hpp>
#include <tenzir/option.hpp>
#include <tenzir/pushdown/plan.hpp>
#include <tenzir/pushdown/sql.hpp>
#include <tenzir/pushdown/sql_source.hpp>
#include <tenzir/time.hpp>

#include <span>
#include <string>
#include <string_view>
#include <vector>

/// The stages that `from_mysql` takes over from the pipeline.
///
/// In `table` mode, `from_mysql` takes over the stages after it that MySQL
/// spells, such as filters, projections, a summary, sorts, and limits, and
/// computes them in its query with MySQL's semantics (see
/// `pushdown::render_plan`). MySQL compares and orders text under the
/// collation of its column, which often ignores case and accents, and with
/// the `PAD SPACE` attribute also trailing spaces, and it sorts `NULL` first.
/// The operators of the stages leave the pipeline.
///
/// Whether MySQL reads a backslash in a string literal as an escape depends on
/// the session's `NO_BACKSLASH_ESCAPES` mode, so strings with a backslash stay
/// in the pipeline, and so do strings and names with a NUL byte.
namespace tenzir::plugins::mysql {

/// Wraps an identifier with backticks, escaping embedded backticks.
auto quote_identifier(std::string_view name) -> std::string;

/// A column of a table, as `information_schema.columns` describes it.
struct TableColumn {
  std::string name;
  /// Whether `SELECT *` includes the column.
  bool visible = true;
};

/// Reads the columns of the table whose name is the only parameter, in order.
inline constexpr auto describe_table_sql
  = std::string_view{"SELECT column_name, extra "
                     "FROM information_schema.columns "
                     "WHERE table_schema = DATABASE() AND table_name = ? "
                     "ORDER BY ordinal_position"};

/// Parses the rows of `describe_table_sql`, skipping rows that do not have the
/// expected shape.
auto parse_table_columns(std::span<std::vector<Option<std::string>> const> rows)
  -> std::vector<TableColumn>;

/// Returns the names of `columns`.
auto column_names(std::span<TableColumn const> columns)
  -> std::vector<std::string>;

/// The selection of a table read.
struct Selection {
  /// The columns to select, as they appear after `SELECT`.
  std::string selection;
  /// The invisible columns that the selection adds for the stages, which the
  /// result leaves out.
  std::vector<std::string> extra;
};

/// Builds the selection of a read of `table` with `columns`, as
/// `pushdown::select_projection` does over the visible columns, so that a
/// projection keeps what `SELECT *` yields. `SELECT *` leaves out invisible
/// columns, which a stage of a derived table above it may read, so the
/// selection adds the invisible columns that the stages of `plan` read, which
/// the result leaves out again.
auto select_columns(Option<ir::OptimizeProjection> const& projection,
                    std::span<TableColumn const> columns,
                    std::span<std::string const> required,
                    std::span<ir::QueryStage const> plan,
                    std::string_view table) -> Selection;

/// Spells the stages of a plan in MySQL SQL.
///
/// It spells times as `'YYYY-MM-DD HH:MM:SS.ffffff'` in UTC, which MySQL
/// reads as the type of the other operand, so the operator sets the session's
/// time zone to UTC. MySQL keeps microseconds, so finer times stay in the
/// pipeline, and so do durations, since MySQL has no interval values. IP
/// addresses become text. It maps these functions: `starts_with`,
/// `ends_with`, `length_bytes`, `length_chars`, `to_lower`, `to_upper`,
/// `trim`, `abs`, and `match_regex`, which MySQL matches with ICU.
class MysqlDialect final : public pushdown::SqlQueryDialect {
public:
  auto quote_identifier(std::string_view name) const -> std::string override;

  auto quote_string(std::string_view text) const
    -> Option<std::string> override;

  auto bind_field(std::span<std::string const> path) const
    -> Option<std::string> override;

  auto render_time(time value) const -> Option<std::string> override;

  auto render_ip(ip value) const -> Option<std::string> override;

  auto
  render_call(std::string_view name, std::span<std::string const> args) const
    -> Option<std::string> override;
};

/// How MySQL spells string literals and comments, for redacting the queries
/// that the operator generates in logs. A user's query may depend on the
/// session's SQL mode, so the operator does not log it.
inline constexpr auto mysql_lexicon = pushdown::SqlLexicon{
  .backslash_escapes = true,
  .double_quoted_strings = true,
  .bracket_identifiers = false,
  .identifier_backslash_escapes = false,
  .hash_comments = true,
  .escape_strings = false,
  .dollar_quoted_strings = false,
};

/// Returns the stages of `chain` that `from_mysql` takes over: the longest
/// prefix that `MysqlDialect` spells. A live read takes over filters only.
auto absorb_stages(ir::QueryChain const& chain, bool live) -> ir::Absorption;

} // namespace tenzir::plugins::mysql
