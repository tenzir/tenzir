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
/// A source in `table` mode decides what to take over from the syntax of the
/// stages (`absorb_prefix`), reads the table's columns when it runs, and then
/// assembles its query from the columns that downstream needs
/// (`select_projection`) and the stages that it took over (`make_table_read`
/// or, for a live read, `make_window_read`). The dialect spells everything.
namespace tenzir::pushdown {

/// Returns the limit of the events that a read with `plan` emits, if its last
/// stage is a limit.
auto final_limit(std::span<ir::QueryStage const> plan) -> Option<uint64_t>;

/// Returns whether `expr` computes with a time literal, as in
/// `ts - 2024-01-01`. A dialect that spells a time as a string, which the
/// target casts to the type of the other operand of a comparison, cannot
/// spell such arithmetic, since the target cannot pick an operator for it.
auto computes_with_time(ast::expression const& expr) -> bool;

/// What a source may take over beyond what its dialect spells.
struct AbsorbOptions {
  /// The name of the target in the reasons for declining, such as `DuckDB`.
  std::string_view target;
  /// Whether the source takes over filters only, as a live read does, which
  /// reads each window anew and counts events across windows.
  bool filters_only = false;
  /// Returns the reason why the source must not take over a stage, or `None`
  /// if it may. It applies before the dialect.
  std::function<auto(ir::QueryStage const&)->Option<std::string>> veto = {};
};

/// Returns whether a source spells all of `stages`, which start right after
/// the source.
using SpellsStages = std::function<auto(std::span<ir::QueryStage const>)->bool>;

/// Returns the stages of `chain` that a source takes over: the longest prefix
/// that `spells` accepts, up to the first stage that `options` rules out, with
/// the reason for declining the rest.
auto absorb_prefix(ir::QueryChain const& chain, SpellsStages const& spells,
                   AbsorbOptions const& options) -> ir::Absorption;

/// Returns the stages of `chain` that a source takes over: the longest prefix
/// that `dialect` spells over a table, up to the first stage that `options`
/// rules out, with the reason for declining the rest.
auto absorb_prefix(ir::QueryChain const& chain, SqlQueryDialect const& dialect,
                   AbsorbOptions const& options) -> ir::Absorption;

/// Returns whether two identifiers name the same column on a target that may
/// compare them without regard to case. Use it to reject a name that the
/// query introduces, which must not name any column of the table.
auto identifiers_may_collide(std::string_view lhs, std::string_view rhs)
  -> bool;

/// Builds the selection of a table read, as it appears after `SELECT`, from
/// the columns that downstream asks for in `projection`, which name the
/// top-level `columns` of the table, together with the `required` columns,
/// such as a tracking column.
///
/// Returns the top-level fields that the stages of `plan` read, or `None` if
/// they may read any field, as through `this`. A stage after a selection reads
/// the fields that the selection names, which may coincide with columns.
auto plan_inputs(std::span<ir::QueryStage const> plan)
  -> Option<std::vector<std::string>>;

/// Spells a selected column, as it appears after `SELECT`.
using SpellColumn = std::function<auto(std::string const&)->std::string>;

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
/// Without a projection, or if `plan` drops fields, which keeps every other
/// column, the selection is `*`. Otherwise, `spell` spells each selected
/// column, and defaults to quoting its name.
auto select_projection(Option<ir::OptimizeProjection> const& projection,
                       std::span<std::string const> columns,
                       std::span<std::string const> required,
                       std::span<ir::QueryStage const> plan,
                       SqlQueryDialect const& dialect,
                       SpellColumn const& spell = {}) -> std::string;

/// The query that reads a table, and how its result becomes events.
struct TableRead {
  std::string sql;
  SqlLayout layout;
};

/// Builds the query that reads `selection`, as it appears after `SELECT`, from
/// `table`, keeping the rows where all of the rendered predicates in `where`
/// hold, and computes `plan` on top of that. Fails if a name that the query
/// introduces may collide with one of the table's `columns`.
///
/// If `dialect.filters_in_projection()` holds and `selection` is not `*`, the
/// filters at the start of `plan` join `where`, so that they constrain the
/// table itself rather than the projection of it.
auto make_table_read(std::string_view table, std::string_view selection,
                     std::span<std::string const> where,
                     std::span<ir::QueryStage const> plan,
                     std::span<std::string const> columns,
                     SqlQueryDialect const& dialect)
  -> Result<TableRead, std::string>;

/// Builds the query that reads one window of a live read: the rows of `table`
/// within `bounds`, as rendered predicates, where all of `filters` hold,
/// ordered by the column `tracking`. Takes only filters, which the target
/// evaluates before it projects to `selection`, so the query need not select
/// the columns that the filters and the order read.
auto make_window_read(std::string_view table, std::string_view selection,
                      std::span<std::string const> bounds,
                      std::span<ir::QueryStage const> filters,
                      std::string_view tracking, SqlQueryDialect const& dialect)
  -> Result<std::string, std::string>;

} // namespace tenzir::pushdown
