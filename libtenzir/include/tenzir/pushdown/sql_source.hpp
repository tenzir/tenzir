//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pushdown/planner.hpp"
#include "tenzir/pushdown/sql.hpp"
#include "tenzir/result.hpp"

#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

/// Shared pieces of SQL sources that take over the stages behind them.
///
/// A source decides what to take over from the syntax of the stages
/// (`sql_planner`), reads the table's columns when it runs, and then plans its
/// query again with the columns that downstream needs (`select_projection`).
/// The dialect spells everything.
namespace tenzir::pushdown {

/// What a SQL source reads: a table, a live window of it, which takes over
/// filters only, since it reads each window anew and counts events across
/// windows, or a query of the user, which takes over nothing.
enum class SqlMode { table, live, query };

/// The window of a live read.
struct SqlWindow {
  /// The rendered predicates that bound the window.
  std::vector<std::string> bounds;
  /// The column whose growth marks new rows, which orders the window.
  std::string tracking;
};

/// What a SQL source looked up at run time about the table that it reads.
struct SqlTable {
  /// The table, as it appears after `FROM`.
  std::string from;
  /// The columns of the table, which no name that the query introduces may
  /// name.
  std::vector<std::string> columns = {};
  /// The columns to select, as they appear after `SELECT`.
  std::string selection = "*";
  /// The window of a live read, and only of one.
  Option<SqlWindow> window = None{};
};

/// The query that reads a table, and how its result becomes events.
struct TableRead {
  std::string sql;
  Layout layout;
};

/// Plans the read of `table` that computes `stages` in `dialect`, or from
/// the syntax of the stages alone if `table` is `nullptr`. The filters at the
/// start of `stages` constrain the table itself in a live read, and also
/// otherwise if `dialect.filters_in_projection()` holds and the selection is
/// not `*`. Fails if a name that the query introduces may name a column.
auto plan_table_read(std::string_view target, SqlQueryDialect const& dialect,
                     SqlMode mode, std::span<ir::QueryStage const> stages,
                     SqlTable const* table) -> Result<TableRead, std::string>;

/// Returns the planner of a read in `dialect`, which must outlive it.
auto sql_planner(std::string target, SqlQueryDialect const& dialect,
                 SqlMode mode) -> Planner<TableRead, SqlTable>;

/// Returns the top-level fields that the `stages` read, or `None` if
/// they may read any field, as through `this`. A stage after a selection reads
/// the fields that the selection names, which may coincide with columns.
auto plan_inputs(std::span<ir::QueryStage const> stages)
  -> Option<std::vector<std::string>>;

/// Spells a selected column, as it appears after `SELECT`.
using SpellColumn = std::function<auto(std::string const&)->std::string>;

/// Builds the selection of a table read, as it appears after `SELECT`, from
/// the columns that downstream asks for in `projection`, which name the
/// top-level `columns` of the table, together with the `required` columns,
/// such as a tracking column.
///
/// A projected name selects the column of the same name, or, without one,
/// every column whose name matches without regard to ASCII case, so that the
/// read keeps what a target that resolves identifiers either way reads. A
/// target may also resolve names beyond ASCII by its own rules, such as a
/// collation that folds `ä` to `Ä` or ignores accents, so if a name matches no
/// column while it or any column goes beyond ASCII, the selection is `*`.
/// Other names that match no column are left to the operators downstream. If
/// nothing remains, the selection keeps the first column, so that the read
/// keeps its rows.
///
/// Without a projection, or if `stages` drop fields, which keeps every other
/// column, the selection is `*`. Otherwise, `spell` spells each selected
/// column, and defaults to quoting its name.
auto select_projection(Option<ir::OptimizeProjection> const& projection,
                       std::span<std::string const> columns,
                       std::span<std::string const> required,
                       std::span<ir::QueryStage const> stages,
                       SqlQueryDialect const& dialect,
                       SpellColumn const& spell = {}) -> std::string;

} // namespace tenzir::pushdown
