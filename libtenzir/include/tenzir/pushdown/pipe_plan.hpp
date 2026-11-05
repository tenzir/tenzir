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
#include "tenzir/pushdown/expr_render.hpp"
#include "tenzir/pushdown/layout.hpp"
#include "tenzir/time.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

/// Plans of stages in a piped query language, such as SPL or PowerQuery.
///
/// A piped language appends one stage per absorbed `ir::QueryStage` to a base
/// query. Each stage sees the output of the one before it, so unlike SQL, a
/// plan needs no nesting. It only tracks which column of the target holds
/// each field that the next stage sees, as a name environment:
///
/// - Before any stage that enumerates its output, the fields are the unknown
///   fields of the events of the base query, and a field binds by its
///   top-level name. A drop excludes names from them.
/// - A select or an aggregation enumerates its output fields, each with the
///   column that holds it. A rename only changes the environment, and a
///   column that a command cannot name gets an alias first.
///
/// The result of a plan becomes events as its `Layout` describes. Every
/// absorbed stage runs with the semantics of the target, and a filter renders
/// as a whole or not at all.
namespace tenzir::pushdown {

/// A column that a stage assigns from another column before it uses it, as
/// in `eval name='input'`.
struct PipeAlias {
  /// The name that the query introduces.
  std::string name;
  /// The column that holds the value.
  std::string input;
};

/// An aggregation function of an aggregate stage.
struct PipeFunction {
  /// The result column.
  std::string name;
  /// The spelling of the function, from `PipeQueryDialect::render_aggregate`.
  std::string call;
};

/// A key of a sort stage.
struct PipeSortKey {
  std::string column;
  bool descending = false;
};

/// The spelling of the stages of a piped query language.
///
/// Every stage is spelled with its leading separator, such as `\n| where `, so
/// that the plan can append it as is. A speller vetoes a stage by returning
/// `None`, which ends the plan before that stage. Stages receive the columns
/// that they read as the target names them, which the dialect spells with
/// `name_column` or `bind_column`.
class PipeQueryDialect : public ExpressionDialect {
public:
  /// Spells a reference to `column` in an expression, or returns `None` if an
  /// expression cannot read it.
  virtual auto bind_column(std::string_view column) const -> Option<std::string>
    = 0;

  /// Spells `column` as an argument of a command, such as in a list of fields
  /// to keep, or returns `None` if a command cannot name it. A select or an
  /// aggregation then assigns such a column to an alias first.
  virtual auto name_column(std::string_view column) const -> Option<std::string>
    = 0;

  /// A name that the query introduces, such as `tenzir_column_0`. The plan
  /// skips names that the stages reference.
  virtual auto generated_name(std::string_view kind, size_t index) const
    -> std::string
    = 0;

  /// Spells the conjunct of a filter that precedes every other stage as a
  /// term of the base query, or returns `None` if it stays in a filter stage.
  /// Called only with `PipeOptions::extend_base`. Defaults to `None`.
  virtual auto base_term(ast::expression const& conjunct) const
    -> Option<std::string>;

  /// Spells the stage that keeps the rows where the rendered `predicate`
  /// holds.
  virtual auto filter_stage(std::string_view predicate) const
    -> Option<std::string>
    = 0;

  /// Spells the stage that assigns `aliases` and then keeps exactly the
  /// distinct `columns`, which are not empty.
  virtual auto select_stage(std::span<PipeAlias const> aliases,
                            std::span<std::string const> columns) const
    -> Option<std::string>
    = 0;

  /// Spells the stage that removes the top-level `columns`, which are not
  /// empty, from rows whose columns are unknown.
  virtual auto drop_stage(std::span<std::string const> columns) const
    -> Option<std::string>
    = 0;

  /// Spells the call of the TQL aggregation function `function` on `column`,
  /// if any, or returns `None` if the target does not compute it. Calls that
  /// spell the same compute the same column.
  virtual auto render_aggregate(std::string_view function,
                                Option<std::string_view> column) const
    -> Option<std::string>
    = 0;

  /// Spells the stage that assigns `aliases`, and then computes `functions`,
  /// which are not empty, per group of the distinct `keys`, or once without
  /// keys.
  virtual auto aggregate_stage(std::span<PipeAlias const> aliases,
                               std::span<PipeFunction const> functions,
                               std::span<std::string const> keys) const
    -> Option<std::string>
    = 0;

  /// Spells the stage that orders the rows by `keys`, which are not empty,
  /// and keeps at most `bound` rows, if set.
  virtual auto
  sort_stage(std::span<PipeSortKey const> keys, Option<uint64_t> bound) const
    -> Option<std::string>
    = 0;

  /// Spells the stage that keeps the first `count` rows.
  virtual auto limit_stage(uint64_t count) const -> Option<std::string> = 0;
};

/// What a plan may do beyond what its dialect spells.
struct PipeOptions {
  /// Whether the conjuncts of the filters that precede every other stage may
  /// extend the base query, as terms from `PipeQueryDialect::base_term`.
  bool extend_base = false;
  /// The column of the base query that holds the time of its events, whose
  /// comparisons with time literals narrow `PipeQuery::window`.
  Option<std::string> time_column = None{};
};

/// The bounds of the time of the input events that pass the filters of a
/// plan, both inclusive, as far as the filters tell.
struct TimeWindow {
  Option<time> begin = None{};
  Option<time> end = None{};
};

/// The stages that compute a plan, and how their result becomes events.
struct PipeQuery {
  /// Terms that extend the base query, from the leading filters.
  std::vector<std::string> base_terms;
  /// The stages to append to the base query, each with its separator.
  std::string stages;
  /// How the result rows become events. Without fields, the rows are the
  /// events of the base query, possibly without some of their fields.
  Layout layout;
  /// Whether the plan contains an aggregation.
  bool aggregates = false;
  /// The columns that hold the result of a `count`, which decode as integers.
  std::vector<std::string> counts;
  /// The time window of the input events from the filters that precede every
  /// aggregation, sort, and limit, if `PipeOptions::time_column` is set.
  TimeWindow window;
  /// The names that the stages reference or introduce, which a name that the
  /// source adds to the query must avoid.
  std::vector<std::string> names;
};

/// Returns a name of `kind` for the source to add to `query`, which avoids
/// every name of `query` and joins them.
auto fresh_name(PipeQuery& query, PipeQueryDialect const& dialect,
                std::string_view kind) -> std::string;

/// Spells `stages` as stages of `dialect` that follow a base query, or returns
/// `None` if `dialect` vetoes some stage. It decides from the syntax of the
/// stages alone, so that a source can decide what to take over at plan time.
///
/// Filters render with `ExpressionRenderer`. A select binds all of its inputs
/// against the fields before it, and a select without fields has no
/// spelling, since a row without fields may vanish. A drop of enumerated
/// fields only changes the layout, and one that leaves no field has no
/// spelling either. A sort orders by fields that a command can name, and
/// needs a positive bound, if any.
auto render_pipe(std::span<ir::QueryStage const> stages,
                 PipeQueryDialect const& dialect, PipeOptions const& options)
  -> Option<PipeQuery>;

} // namespace tenzir::pushdown
