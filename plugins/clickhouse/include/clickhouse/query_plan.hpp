//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "clickhouse/catch_all_reader.hpp"
#include "clickhouse/table_description.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pushdown/plan.hpp"
#include "tenzir/pushdown/sql.hpp"
#include "tenzir/pushdown/sql_source.hpp"
#include "tenzir/result.hpp"
#include "tenzir/tql2/ast.hpp"

#include <span>
#include <string>
#include <string_view>
#include <vector>

/// The stages that `from_clickhouse` takes over from the pipeline.
///
/// In `table` mode, `from_clickhouse` takes over the stages after it that
/// ClickHouse spells, such as filters, projections, a summary, sorts, and
/// limits, and computes them in its query with ClickHouse's semantics (see
/// `pushdown::render_plan`). The operators of the stages leave the pipeline.
///
/// It decides what to take over from the syntax of the stages alone. When it
/// runs, it reads the table's columns and binds every field that the stages
/// read from the table to the column that holds it (see `TableBinding`). A
/// field that it cannot bind fails the read before the query runs.
namespace tenzir::plugins::clickhouse {

/// A field of a table, as the query of a read spells it.
struct BoundField {
  /// The expression that reads the field.
  std::string sql;
  /// Whether the field lies below a `JSON` column and thus has ClickHouse's
  /// type `Dynamic`, which the operator cannot read as a result column.
  bool dynamic = false;
  /// For a field below a `JSON` column, the expression that reads the object
  /// below its path, such as `j`.^`a`. ClickHouse stores no value at the path
  /// of an object, only at the paths below it, so the value of an object is
  /// `NULL`, while this object has paths.
  Option<std::string> object = None{};
};

/// How the fields of a table map to its columns, as far as the query of a read
/// can address them.
///
/// In a table without a catch-all column, a top-level field is the column of
/// the same name, which must match exactly, as ClickHouse resolves names, a
/// field below a named `Tuple`
/// is its element, and a field below a `JSON` column without typed paths is
/// the value at its path. A name that the table lacks spells as a column, so
/// that ClickHouse rejects it.
///
/// In a table with a catch-all column, a field is the value of the column that
/// `CatchAllLayout::origin` names for it: a column, a value below a `JSON`
/// column, or a value below the catch-all column. A field without a single
/// origin does not bind.
///
/// Neither binds a generated column, which `SELECT *` leaves out depending on
/// the session's settings, a column whose type the operator cannot read, a
/// whole `JSON` column below which the operator reconstructs a record, a path
/// below a `JSON` column with typed or skipped paths, or a path below a
/// `JSON` column that a column of the same dotted name shadows.
class TableBinding {
public:
  TableBinding(std::vector<ColumnDescription> description,
               Option<CatchAllLayout> catch_all);

  /// Returns the expression for the field at `path`, or the reason why it
  /// does not bind.
  auto bind(std::span<std::string const> path) const
    -> Result<BoundField, std::string>;

  /// The layout of the table, if it has a catch-all column.
  auto catch_all() const -> Option<CatchAllLayout const&>;

private:
  /// Binds the field at `path` below `column`.
  auto bind_column(ColumnDescription const& column,
                   std::span<std::string const> path) const
    -> Result<BoundField, std::string>;

  /// Binds the value at the path of `keys` below the `JSON` column `column`.
  auto bind_json(ColumnDescription const& column,
                 std::span<std::string const> keys) const
    -> Result<BoundField, std::string>;

  auto find(std::string_view name) const -> ColumnDescription const*;

  std::vector<ColumnDescription> description_;
  Option<CatchAllLayout> catch_all_;
};

/// The stage of a plan, or the field that it reads, that does not bind to a
/// table.
struct BindError {
  /// What does not bind.
  enum class Kind {
    /// A field, which `subject` names.
    field,
    /// A stage that reads no field, which `subject` names, such as
    /// `summarize`.
    stage,
    /// A sort by a field below a `JSON` column, which `subject` names, whose
    /// order the query would carry through a derived table for a later stage.
    sort,
  };
  Kind kind = Kind::field;
  /// The field path, as TQL spells it, or the name of the stage.
  std::string subject;
  std::string reason;
};

/// The fields that the stages of a plan read from a table, bound to it.
struct BoundPlan {
  detail::flat_map<std::vector<std::string>, BoundField> fields;
  /// Whether a stage reads a value below a `JSON` column.
  bool reads_dynamic = false;
  /// Whether a sort orders by a value below a `JSON` column.
  bool sorts_dynamic = false;
  /// Whether the plan summarizes.
  bool summarizes = false;
};

/// Binds the fields that the stages of `plan` read from the table, up to the
/// first stage that enumerates its output, such as a `select` or a summary.
///
/// A filter and a sort bind any field. A selection and a summary bind no value
/// below a `JSON` column. Neither does a sort whose order a later stage needs
/// after a limit or a projection, since the query then carries the sort keys
/// in result columns. A table with a catch-all column supports plans that
/// keep whole rows only, so a projection, a drop, or a summary fails there:
/// a field of the reconstructed events may live in any column.
auto bind_plan(TableBinding const& table, std::span<ir::QueryStage const> plan)
  -> Result<BoundPlan, BindError>;

/// Spells the stages of a plan in ClickHouse SQL.
///
/// It quotes identifiers with backticks and strings with single quotes, both
/// with backslash escapes. Times spell as `fromUnixTimestamp64Nano` of their
/// nanoseconds since the epoch in UTC, and IP addresses as `toIPv4` or
/// `toIPv6` of their text. It maps `starts_with`, `ends_with`,
/// `length_bytes`, `length_chars`, `to_lower`, `to_upper`, `trim`, `abs`, and
/// `match_regex`, and the aggregation functions `count`, `min`, and `max`,
/// which yield `null` for no rows, as in TQL.
///
/// Without a `BoundPlan`, it binds every field from its syntax alone, as the
/// decision of what to take over requires. With one, it spells each field of
/// the table as the plan bound it, and a null check of a field below a `JSON`
/// column also considers the object below its path, which ClickHouse reads as
/// `NULL`: `x != null` holds for a scalar or an object with values, and `x ==
/// null` otherwise. A comparison of such an object with a literal is `NULL`, so
/// it matches no row.
class ClickHouseDialect final : public pushdown::SqlQueryDialect {
public:
  ClickHouseDialect() = default;

  explicit ClickHouseDialect(BoundPlan const& bound) : bound_{&bound} {
  }

  auto quote_identifier(std::string_view name) const -> std::string override;

  auto quote_string(std::string_view text) const
    -> Option<std::string> override;

  auto render_double(double value) const -> std::string override;

  auto render_time(time value) const -> Option<std::string> override;

  auto render_ip(ip value) const -> Option<std::string> override;

  auto bind_field(std::span<std::string const> path) const
    -> Option<std::string> override;

  auto render_binary(ast::binary_expr const& x,
                     pushdown::ExpressionRenderer const& renderer) const
    -> Option<Option<std::string>> override;

  auto
  render_call(std::string_view name, std::span<std::string const> args) const
    -> Option<std::string> override;

  auto
  render_aggregate(std::string_view name, Option<std::string> const& arg) const
    -> Option<std::string> override;

  auto render_exclude(std::span<std::string const> columns) const
    -> Option<std::string> override;

private:
  BoundPlan const* bound_ = nullptr;
};

/// How ClickHouse spells string literals, quoted identifiers, and comments,
/// for redacting the queries that the operator generates in logs.
inline constexpr auto clickhouse_lexicon = pushdown::SqlLexicon{
  .backslash_escapes = true,
  .double_quoted_strings = false,
  .bracket_identifiers = false,
  .identifier_backslash_escapes = true,
  .hash_comments = true,
  .escape_strings = false,
  .dollar_quoted_strings = false,
  .triple_backtick_comments = false,
};

/// Returns the stages of `chain` that `from_clickhouse` takes over: the
/// longest prefix that `ClickHouseDialect` spells.
auto absorb_stages(ir::QueryChain const& chain) -> ir::Absorption;

/// Builds the query that reads `selection`, as it appears after `SELECT`,
/// from `table` and computes `plan` on top of that, with the fields that
/// `bound` bound. Fails if a name that the query introduces may collide with
/// one of the table's `columns`.
auto make_table_read(std::string_view table, std::string_view selection,
                     std::span<ir::QueryStage const> plan,
                     std::span<std::string const> columns,
                     BoundPlan const& bound)
  -> Result<pushdown::TableRead, std::string>;

} // namespace tenzir::plugins::clickhouse
