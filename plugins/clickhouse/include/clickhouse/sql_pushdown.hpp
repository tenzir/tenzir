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
#include "tenzir/box.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/flat_set.hpp"
#include "tenzir/detail/stable_map.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pushdown/column_model.hpp"
#include "tenzir/pushdown/renderer.hpp"
#include "tenzir/pushdown/sql.hpp"

#include <span>
#include <string>
#include <string_view>
#include <vector>

/// Pushdown of optimizer hints into the `SELECT` of `from_clickhouse`.
///
/// The optimizer hands `from_clickhouse` the entire filter chain that follows
/// it, plus an optional limit and projection. This module rewrites the
/// `SELECT` that the operator sends so that ClickHouse does as much of that
/// work as possible. The translation of filters is shared with other sources
/// (see `tenzir/pushdown/translate.hpp`). This module describes a ClickHouse
/// table to it, spells the result in ClickHouse SQL, and assembles the query.
///
/// TODO: Pushdown into a user-provided `sql` query, which requires parsing
/// SQL.
namespace tenzir::plugins::clickhouse {

/// The columns of a ClickHouse table.
class SqlSchema {
public:
  /// Registers a `DESCRIBE TABLE` row. Named tuple elements become nested
  /// paths, so `t Tuple(a Int64)` makes `t.a` addressable. `Nullable` and
  /// `LowCardinality` wrappers are transparent.
  ///
  /// A column whose type `from_clickhouse` cannot decode is absent from the
  /// local result, and so is a tuple with any such element. Such a column has
  /// no addressable paths: a predicate on one of its elements would match rows
  /// in SQL that the local evaluation never sees, and a narrowed projection
  /// would make an element decodable that the whole column is not.
  ///
  /// A `generated` column (`ALIAS`, `MATERIALIZED`, or `EPHEMERAL`) is not part
  /// of `SELECT *` unless session settings include it, so the local evaluation
  /// may or may not see it. Predicates on it stay local, and a projection that
  /// names it selects every column, which reproduces `SELECT *` either way.
  auto add_column(std::string_view name, std::string_view type,
                  bool generated = false) -> void;

  /// Makes the scalar column `name` addressable at the nested `path`, where
  /// the reconstruction of a catch-all table places it. Tuples and types that
  /// have no TQL counterpart stay unaddressable.
  auto add_placed_column(std::string_view name, std::string_view type,
                         std::vector<std::string> path) -> void;

  /// Makes the field at `path` addressable as the value at `json_path` below
  /// the `JSON` column `column`, read with `json_type_escape_dots_in_keys=1`.
  ///
  /// The field has a `DynamicType`, so predicates on it can only narrow the
  /// rows as a prefilter. `ClickHouseRenderer` compares the text of the
  /// stored value, which TQL sees as the same text, number, or boolean. A
  /// path with an empty component stays unaddressable.
  auto add_json_field(std::vector<std::string> path, std::string_view column,
                      std::span<const std::string> json_path) -> void;

  /// The top-level columns in table order, including generated ones.
  auto columns() const -> std::span<const std::string>;

  /// Returns whether the top-level column `name` is generated.
  auto is_generated(std::string_view name) const -> bool;

  /// The columns that predicates may reference, as TQL sees them. A column
  /// whose type has no unambiguous TQL counterpart is absent.
  auto model() const -> pushdown::ColumnModel const&;

  /// Renders the `SELECT` expression for the top-level column `name` that
  /// yields only the nested `paths` below it, or `None` if the column should
  /// be selected whole. Each path is relative to the column. The expression
  /// rebuilds the tuple structure with `CAST(tuple(...))`, so that the result
  /// decodes to the same nested record with the other elements left out.
  auto render_projection(std::string_view name,
                         std::span<const std::vector<std::string>> paths) const
    -> Option<std::string>;

  /// A field below a `JSON` column.
  struct JsonField {
    /// The name of the `JSON` column.
    std::string column;
    /// The keys of the path below the column, with the dots in keys escaped
    /// as `%2E`.
    std::vector<std::string> keys;
  };

  /// Returns the `JSON` field at `path`, if `add_json_field` registered one.
  auto json_field(std::span<const std::string> path) const
    -> Option<JsonField const&>;

  /// A field below a `JSON` column that stays unaddressable, because a column
  /// named like the start of its subcolumn shadows it.
  struct ShadowedField {
    /// The path of the field in the reconstructed events.
    std::vector<std::string> path;
    /// The column that shadows the field.
    std::string column;
  };

  /// Records that the column `column` shadows the field at `path`.
  auto add_shadowed_field(std::vector<std::string> path,
                          std::string_view column) -> void;

  /// The fields that `add_shadowed_field` recorded.
  auto shadowed_fields() const -> std::span<const ShadowedField>;

  /// Returns the column that `add_placed_column` placed at `path`.
  auto placed_column(std::span<const std::string> path) const
    -> Option<std::string const&>;

private:
  /// A node of a column's type structure. Tuples have children; every other
  /// type is a leaf.
  struct Node {
    /// The ClickHouse type text, with insignificant whitespace removed.
    std::string type;
    /// Named tuple elements in declaration order. Boxed because the type
    /// recurses through the map's value type.
    detail::stable_map<std::string, Box<Node>> children;
  };

  /// Builds the node for a column of `type` at `path`, and adds every
  /// comparable leaf below it to the model.
  auto make_node(std::string_view type, std::vector<std::string>& path) -> Node;

  auto find_node(std::span<const std::string> path) const -> Node const*;

  std::vector<std::string> columns_;
  detail::flat_set<std::string> generated_;
  detail::stable_map<std::string, Node> nodes_;
  detail::flat_map<std::vector<std::string>, std::string> placed_;
  detail::flat_map<std::vector<std::string>, JsonField> json_fields_;
  std::vector<ShadowedField> shadowed_;
  pushdown::ColumnModel model_;
};

/// Describes a catch-all table to filter pushdown, as far as the fields of the
/// reconstructed events have a single origin (see `CatchAllLayout::origin`).
///
/// Plain and dotted scalar columns are addressable at their paths. If
/// `json_fields` is set, the fields that `filter` references below the
/// catch-all column or another `JSON` column are addressable too, as
/// `add_json_field` describes. That excludes `JSON` columns with typed or
/// skipped paths, since a typed path may hold a record of TQL without paths
/// below it in ClickHouse. It also excludes the fields that a column named
/// like the start of their subcolumn shadows, such as the fields below `x` in
/// the catch-all column `event` next to a column `event.x`: ClickHouse resolves
/// `event`.`x` to that column. `SqlSchema::shadowed_fields` lists them.
auto make_catch_all_schema(std::span<ColumnDescription const> description,
                           CatchAllLayout const& layout,
                           ir::OptimizeFilter const& filter, bool json_fields)
  -> SqlSchema;

/// The server that answers a rendered query.
enum class SqlTarget {
  /// ClickHouse.
  clickhouse,
  /// Hydrolix, which answers ClickHouse SQL through a ClickHouse query engine.
  /// It derives the time range of a query from the comparisons of the bare
  /// primary timestamp column and rejects or misreads the range that a time
  /// guard spans, but it ignores comparisons of a function of the column.
  hydrolix,
};

/// Spells the pushdown IR in ClickHouse SQL.
///
/// ClickHouse computes integer arithmetic in a type that holds the exact
/// result and floating-point arithmetic in `Float64`, so the default spelling
/// of arithmetic delivers the result types of the IR.
///
/// With a `schema`, columns placed at nested paths render as their columns,
/// and fields below `JSON` columns render by their text: a comparison with a
/// string or boolean compares the text, and one with a number parses the text
/// as `Float64` beforehand, which needs `precise_float_parsing=1`. Neither
/// throws for any stored type, and both hold for every value that TQL sees
/// as the literal, since ClickHouse sends the same text as JSON. Only the
/// prefilters that translation emits for a `DynamicType` render.
class ClickHouseRenderer final : public pushdown::SqlRenderer {
public:
  explicit ClickHouseRenderer(SqlTarget target = SqlTarget::clickhouse,
                              SqlSchema const* schema = nullptr)
    : target_{target}, schema_{schema} {
  }

private:
  auto quote_identifier(std::string_view name) const -> std::string override;

  auto quote_string(std::string_view text) const -> std::string override;

  auto render_double(double x) const -> std::string override;

  auto render_enum(pushdown::EnumLabel const& x) const
    -> Option<Fragment> override;

  auto render_time(pushdown::TimeValue const& x) const
    -> Option<Fragment> override;

  auto render_ip(pushdown::IpValue const& x) const -> Option<Fragment> override;

  auto render_column(std::span<std::string const> path) const
    -> Option<Fragment> override;

  auto
  render_call(pushdown::Call const& x, std::span<Fragment const> args) const
    -> Option<Fragment> override;

  auto render_comparison(pushdown::Binary const& x, Fragment const& left,
                         Fragment const& right) const
    -> Option<Fragment> override;

  auto render_arithmetic(pushdown::Binary const& x, Fragment const& left,
                         Fragment const& right) const
    -> Option<Fragment> override;

  auto render_in(pushdown::In const& x, Fragment const& expr,
                 std::span<Fragment const> list) const
    -> Option<Fragment> override;

  auto render_is_null(pushdown::IsNull const& x, Fragment const& operand) const
    -> Option<Fragment> override;

  auto render_conditional(Fragment const& condition, Fragment const& then,
                          Fragment const& otherwise) const
    -> Option<Fragment> override;

  /// Returns the `JSON` field that `x` names, if it is a column.
  auto json_field(pushdown::Expr const& x) const
    -> Option<SqlSchema::JsonField const&>;

  /// Spells the stored value of a `JSON` field.
  auto render_json_value(SqlSchema::JsonField const& field) const -> Fragment;

  /// Spells the comparison of the text of a `JSON` field with `literal`.
  auto render_json_comparison(pushdown::Binary const& x, Fragment const& text,
                              pushdown::Literal const& literal,
                              Fragment const& rendered) const
    -> Option<Fragment>;

  SqlTarget target_;
  SqlSchema const* schema_;
};

/// Renders the `SELECT` for `table`. `schema` is required when `projection`
/// is set; without a schema, the projection is ignored. Each path in the
/// projection selects its top-level column, narrowed to the requested tuple
/// elements when the path is nested; if no column survives, or a path names a
/// generated column, every column is selected.
auto make_select_query(std::string_view table, SqlSchema const* schema,
                       Option<ir::OptimizeProjection> const& projection,
                       std::span<const std::string> where,
                       Option<uint64_t> limit) -> std::string;

/// Plans a read of a table with `schema`: pushes the predicates of `filter`
/// that ClickHouse evaluates exactly like TQL, rendered for `target`, and
/// selects the columns that `projection` names, as `make_select_query` does.
auto make_table_plan(SqlSchema const& schema, SqlTarget target,
                     ir::OptimizeFilter filter,
                     Option<ir::OptimizeProjection> const& projection,
                     registry const& reg) -> pushdown::TablePlan;

/// Renders the `SELECT` that reads `table` according to `plan`, with `LIMIT
/// limit` if every predicate was pushed.
auto make_table_query(pushdown::TablePlan const& plan, std::string_view table,
                      Option<uint64_t> limit) -> std::string;

/// Whether `make_aggregate_query` supports every function of `aggregation`.
/// Whether it supports their inputs depends on the table's schema.
auto supports_aggregate_functions(ir::OptimizeAggregation const& aggregation)
  -> bool;

/// Renders a query that computes `aggregation` over `table`, keeping the rows
/// where all of the rendered predicates in `where` hold. Returns `None` if
/// ClickHouse cannot compute some output column exactly as TQL does, in which
/// case the operator aggregates locally.
///
/// The query selects output column `i` as the flat result column
/// `pushdown::aggregation_column(i)`, which `pushdown::nest_aggregation`
/// arranges into the output fields. ClickHouse resolves a name to an alias
/// before a column, so the query is `None` if the table has a column of that
/// name. Each column is computed from the value that TQL sees for its input
/// field (see `pushdown::translate_field`). It supports:
///
/// - Group keys on booleans, integers, strings, enums, UUIDs, IP addresses,
///   and temporal values. TQL compares each of them like `GROUP BY` does.
///   Floating-point keys stay local, because `GROUP BY` equates `NaN` with
///   itself and `-0.0` with `0.0`.
/// - `count()`, and `count(x)` on any column of the model, cast to `Int64`,
///   which TQL reads as `int`.
/// - `min(x)` and `max(x)` on integers and temporal values, with the `OrNull`
///   combinator so that an aggregation without group keys over no rows yields
///   `NULL` instead of the type's default value.
///
/// The query renders the group keys and inputs for `target`.
auto make_aggregate_query(ir::OptimizeAggregation const& aggregation,
                          SqlSchema const& schema, std::string_view table,
                          std::span<const std::string> where, SqlTarget target)
  -> Option<std::string>;

/// Quotes `name` as a ClickHouse identifier using backticks.
auto quote_sql_identifier(std::string_view name) -> std::string;

/// Quotes `text` as a ClickHouse string literal using single quotes.
auto quote_sql_string(std::string_view text) -> std::string;

} // namespace tenzir::plugins::clickhouse
