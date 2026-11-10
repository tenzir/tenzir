//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/query_plan.hpp"

#include "clickhouse/arguments.hpp"
#include "clickhouse/block_decoding.hpp"
#include "clickhouse/sql_pushdown.hpp"
#include "clickhouse/transformers.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/pushdown/expr_render.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <clickhouse/columns/factory.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <cctype>

namespace tenzir::plugins::clickhouse {

namespace {

/// Whether the default kind of a column makes it generated, so that `SELECT *`
/// includes it only depending on the session's settings.
auto is_generated(ColumnDescription const& column) -> bool {
  return column.default_kind == "ALIAS" or column.default_kind == "MATERIALIZED"
         or column.default_kind == "EPHEMERAL";
}

/// Strips `Nullable(...)` and `LowCardinality(...)` wrappers from a type
/// without non-significant whitespace.
auto unwrap(std::string_view type) -> std::string_view {
  while (true) {
    if (auto inner = unwrap_clickhouse_type_call(type, "Nullable")) {
      type = *inner;
    } else if (auto inner
               = unwrap_clickhouse_type_call(type, "LowCardinality")) {
      type = *inner;
    } else {
      return type;
    }
  }
}

auto is_json(std::string_view bare) -> bool {
  return bare == "JSON" or unwrap_clickhouse_type_call(bare, "JSON");
}

/// Whether the events decoder reads a column of `type`.
auto decodes(std::string_view type) -> bool {
  try {
    auto column = ::clickhouse::CreateColumnByType(std::string{type});
    return column and classify(column->Type()).is_some();
  } catch (std::exception const&) {
    return false;
  }
}

/// Returns the type of the named element `name` of the tuple type `bare`, or
/// `None` if `bare` has no such element.
auto tuple_element(std::string_view bare, std::string_view name)
  -> Option<std::string_view> {
  auto elements = unwrap_clickhouse_type_call(bare, "Tuple");
  if (not elements) {
    return None{};
  }
  for (auto element : split_top_level_clickhouse_type_arguments(*elements)) {
    auto split = find_top_level_clickhouse_type_space(element);
    if (split == std::string_view::npos) {
      continue;
    }
    auto element_name
      = unquote_identifier_component(detail::trim(element.substr(0, split)));
    if (element_name == name) {
      return detail::trim(element.substr(split + 1));
    }
  }
  return None{};
}

/// Spells the element `name` of the tuple `value`. Unlike `value`.`name`, it
/// cannot resolve to a column whose name contains a dot.
auto spell_element(std::string const& value, std::string_view name)
  -> std::string {
  return fmt::format("tupleElement({}, {})", value, quote_sql_string(name));
}

/// Spells a field path as TQL does, such as `a.b` or `this["a.b"]`.
auto format_path(std::span<std::string const> path) -> std::string {
  auto const plain = [](std::string const& segment) {
    return not segment.empty()
           and (std::isalpha(static_cast<unsigned char>(segment.front())) != 0
                or segment.front() == '_')
           and std::ranges::all_of(segment, [](char c) {
                 return std::isalnum(static_cast<unsigned char>(c)) != 0
                        or c == '_';
               });
  };
  auto result = std::string{};
  for (auto const& segment : path) {
    if (plain(segment)) {
      if (not result.empty()) {
        result += '.';
      }
      result += segment;
    } else {
      if (result.empty()) {
        result = "this";
      }
      result += fmt::format("[{:?}]", segment);
    }
  }
  return result;
}

/// The name of a stage in the error that it does not bind.
auto stage_name(ir::QueryStage const& stage) -> std::string_view {
  return match(
    stage.kind,
    [](ir::QueryStage::Filter const&) {
      return "where";
    },
    [](ir::QueryStage::Select const&) {
      return "select";
    },
    [](ir::QueryStage::Drop const&) {
      return "drop";
    },
    [](ir::QueryStage::Aggregate const&) {
      return "summarize";
    },
    [](ir::QueryStage::Sort const&) {
      return "sort";
    },
    [](ir::QueryStage::Limit const&) {
      return "head";
    });
}

} // namespace

// -- TableBinding -------------------------------------------------------------

TableBinding::TableBinding(std::vector<ColumnDescription> description,
                           Option<CatchAllLayout> catch_all)
  : description_{std::move(description)}, catch_all_{std::move(catch_all)} {
}

auto TableBinding::catch_all() const -> Option<CatchAllLayout const&> {
  if (not catch_all_) {
    return None{};
  }
  return *catch_all_;
}

auto TableBinding::find(std::string_view name) const
  -> ColumnDescription const* {
  auto it = std::ranges::find(description_, name, &ColumnDescription::name);
  return it == description_.end() ? nullptr : &*it;
}

auto TableBinding::bind(std::span<std::string const> path) const
  -> Result<BoundField, std::string> {
  TENZIR_ASSERT(not path.empty());
  if (std::ranges::any_of(path, &std::string::empty)) {
    return Err{std::string{"the field has an empty name"}};
  }
  if (catch_all_) {
    auto origin = catch_all_->origin(path);
    if (not origin) {
      return Err{std::string{
        "the field has no single column in the table: several columns hold "
        "parts of it, or a generated column may hold it"}};
    }
    auto const* column = find(origin->column);
    TENZIR_ASSERT(column);
    if (origin->json) {
      if (origin->path.empty()) {
        return Err{fmt::format("the field is the whole `JSON` column `{}`, "
                               "from which the operator reconstructs a record",
                               origin->column)};
      }
      return bind_json(*column, origin->path);
    }
    return bind_column(*column, origin->path);
  }
  auto const* column = find(path.front());
  if (not column) {
    // ClickHouse rejects a column that the table lacks.
    auto result = quote_sql_identifier(path.front());
    for (auto const& segment : path.subspan(1)) {
      result = spell_element(result, segment);
    }
    return BoundField{
      .sql = std::move(result), .dynamic = false, .object = None{}};
  }
  return bind_column(*column, path.subspan(1));
}

auto TableBinding::bind_column(ColumnDescription const& column,
                               std::span<std::string const> path) const
  -> Result<BoundField, std::string> {
  if (is_generated(column)) {
    return Err{fmt::format("`{}` is an `{}` column, which `SELECT *` includes "
                           "only depending on the session's settings",
                           column.name, column.default_kind)};
  }
  auto const type = remove_non_significant_whitespace(column.type);
  auto bare = unwrap(type);
  if (is_json(bare)) {
    if (path.empty()) {
      // The operator reads a whole `JSON` column as text.
      return BoundField{.sql = quote_sql_identifier(column.name),
                        .dynamic = false,
                        .object = None{}};
    }
    return bind_json(column, path);
  }
  // A tuple element reads on its own, even if a sibling element does not
  // decode, since a projection narrows the tuple to the element.
  if (path.empty() and not decodes(type)) {
    return Err{fmt::format("the column `{}` has the type `{}`, which the "
                           "operator cannot read",
                           column.name, column.type)};
  }
  auto result = quote_sql_identifier(column.name);
  auto parent = column.name;
  auto selected = std::string_view{type};
  for (auto const& segment : path) {
    auto element = tuple_element(bare, segment);
    if (not element) {
      if (not unwrap_clickhouse_type_call(bare, "Tuple")) {
        return Err{fmt::format("`{}` has the type `{}`, which has no fields",
                               parent, bare)};
      }
      return Err{
        fmt::format("the tuple `{}` has no element `{}`", parent, segment)};
    }
    result = spell_element(result, segment);
    parent = fmt::format("{}.{}", parent, segment);
    selected = *element;
    bare = unwrap(*element);
  }
  if (not path.empty() and not decodes(selected)) {
    return Err{fmt::format("`{}` has the type `{}`, which the operator cannot "
                           "read",
                           parent, selected)};
  }
  return BoundField{
    .sql = std::move(result), .dynamic = false, .object = None{}};
}

auto TableBinding::bind_json(ColumnDescription const& column,
                             std::span<std::string const> keys) const
  -> Result<BoundField, std::string> {
  TENZIR_ASSERT(not keys.empty());
  auto const type = remove_non_significant_whitespace(column.type);
  auto bare = std::string_view{type};
  if (auto inner = unwrap_clickhouse_type_call(bare, "Nullable")) {
    bare = *inner;
  }
  // A typed path may hold a record of TQL without ClickHouse having paths
  // below it.
  if (not is_lossless_json(bare)) {
    return Err{fmt::format("the `JSON` column `{}` has typed or skipped paths",
                           column.name)};
  }
  // `json_type_escape_dots_in_keys=1` stores a key `a.b` at the path `a%2Eb`.
  auto escaped = std::vector<std::string>{};
  for (auto const& key : keys) {
    escaped.push_back(detail::replace_all(key, ".", "%2E"));
  }
  // ClickHouse resolves `column`.`key`... to a column named like that,
  // joined with dots, instead of the path below the `JSON` column.
  auto name = column.name;
  for (auto const& key : escaped) {
    name += '.';
    name += key;
    if (find(name)) {
      return Err{fmt::format("the column `{}` shadows the field below the "
                             "`JSON` column `{}`",
                             name, column.name)};
    }
  }
  auto result = quote_sql_identifier(column.name);
  // `column`.^`key`... is the object below the path, with the paths below it.
  auto object = quote_sql_identifier(column.name);
  auto separator = std::string_view{".^"};
  for (auto const& key : escaped) {
    result += '.';
    result += quote_sql_identifier(key);
    object += separator;
    object += quote_sql_identifier(key);
    separator = ".";
  }
  return BoundField{
    .sql = std::move(result), .dynamic = true, .object = std::move(object)};
}

// -- bind_plan ----------------------------------------------------------------

auto bind_plan(TableBinding const& table, std::span<ir::QueryStage const> plan)
  -> Result<BoundPlan, BindError> {
  auto result = BoundPlan{};
  result.summarizes = std::ranges::any_of(plan, [](auto const& stage) {
    return is<ir::QueryStage::Aggregate>(stage.kind);
  });
  auto const catch_all = table.catch_all();
  // Binds the field at `path` for a stage that reads it as a result column if
  // `output`, or that sorts by it if `sorts`.
  auto bind = [&](std::span<std::string const> path, bool output,
                  bool sorts) -> Result<void, BindError> {
    auto bound = table.bind(path);
    if (bound.is_err()) {
      return Err{BindError{
        .kind = BindError::Kind::field,
        .subject = format_path(path),
        .reason = std::move(bound).unwrap_err(),
      }};
    }
    auto field = std::move(bound).unwrap();
    if (field.dynamic and output) {
      return Err{BindError{
        .kind = BindError::Kind::field,
        .subject = format_path(path),
        .reason = "the field lies below a `JSON` column and has a dynamic "
                  "type, which the operator cannot read as a result column",
      }};
    }
    result.reads_dynamic = result.reads_dynamic or field.dynamic;
    result.sorts_dynamic = result.sorts_dynamic or (field.dynamic and sorts);
    result.fields.emplace(std::vector<std::string>{path.begin(), path.end()},
                          std::move(field));
    return {};
  };
  // A table with a catch-all column supports stages that keep whole rows.
  auto whole_rows =
    [&](ir::QueryStage const& stage,
        Option<std::span<std::string const>> field) -> Result<void, BindError> {
    if (not catch_all) {
      return {};
    }
    return Err{BindError{
      .kind = field ? BindError::Kind::field : BindError::Kind::stage,
      .subject = field ? format_path(*field) : std::string{stage_name(stage)},
      .reason = fmt::format("`{}` on a table with the catch-all column `{}` "
                            "reads fields that "
                            "may live in any of its columns",
                            stage_name(stage), catch_all->column),
    }};
  };
  for (auto const& stage : plan) {
    // The stages after a projection or a summary read its result columns.
    auto const done = match(
      stage.kind,
      [&](ir::QueryStage::Filter const& x) -> Result<bool, BindError> {
        auto paths = std::vector<std::vector<std::string>>{};
        // The renderer refers to the dialect, which must outlive it.
        auto const dialect = ClickHouseDialect{};
        auto renderer = pushdown::ExpressionRenderer{
          dialect, [&](std::span<std::string const> path) {
            paths.emplace_back(path.begin(), path.end());
            return Option<std::string>{"x"};
          }};
        std::ignore = renderer.predicate(x.predicate);
        for (auto const& path : paths) {
          TRY(bind(path, false, false));
        }
        return false;
      },
      [&](ir::QueryStage::Sort const& x) -> Result<bool, BindError> {
        for (auto const& key : x.keys) {
          TRY(bind(key.field, false, true));
        }
        return false;
      },
      [&](ir::QueryStage::Select const& x) -> Result<bool, BindError> {
        if (x.fields.empty()) {
          TRY(whole_rows(stage, None{}));
        }
        for (auto const& field : x.fields) {
          TRY(whole_rows(stage, std::span{field.input}));
          TRY(bind(field.input, true, false));
        }
        return true;
      },
      [&](ir::QueryStage::Drop const& x) -> Result<bool, BindError> {
        TRY(whole_rows(stage, x.fields.empty()
                                ? Option<std::span<std::string const>>{}
                                : std::span{x.fields.front()}));
        return false;
      },
      [&](ir::QueryStage::Aggregate const& x) -> Result<bool, BindError> {
        auto const first
          = std::ranges::find_if(x.columns, [](auto const& column) {
              return column.input.has_value();
            });
        TRY(whole_rows(stage, first == x.columns.end()
                                ? Option<std::span<std::string const>>{}
                                : std::span{*first->input}));
        for (auto const& column : x.columns) {
          if (column.input) {
            TRY(bind(*column.input, true, false));
          }
        }
        return true;
      },
      [&](ir::QueryStage::Limit const&) -> Result<bool, BindError> {
        return false;
      });
    if (done.is_err()) {
      return Err{std::move(done).unwrap_err()};
    }
    if (done.unwrap()) {
      break;
    }
  }
  // A stage after a limit or a projection reads a derived table, which keeps
  // the order of the sorts before it in result columns that the query
  // returns. The operator cannot read such a column for a value below a
  // `JSON` column, so the sort must be the last stage that needs the order.
  auto dynamic_key = Option<std::vector<std::string>>{};
  auto closed = false;
  auto reads_table = true;
  for (auto const& stage : plan) {
    auto const wraps = is<ir::QueryStage::Filter>(stage.kind)
                       or is<ir::QueryStage::Select>(stage.kind)
                       or is<ir::QueryStage::Drop>(stage.kind)
                       or is<ir::QueryStage::Sort>(stage.kind);
    if (wraps and closed) {
      if (dynamic_key) {
        return Err{BindError{
          .kind = BindError::Kind::sort,
          .subject = format_path(*dynamic_key),
          .reason = "the query would return the order of the sort for the "
                    "stages after it in a column of a dynamic type, which "
                    "the operator cannot read",
        }};
      }
      closed = false;
    }
    match(
      stage.kind,
      [&](ir::QueryStage::Sort const& x) {
        for (auto const& key : x.keys) {
          auto it = result.fields.find(key.field);
          if (reads_table and not dynamic_key and it != result.fields.end()
              and it->second.dynamic) {
            dynamic_key = key.field;
          }
        }
      },
      [&](ir::QueryStage::Select const&) {
        closed = true;
        reads_table = false;
      },
      [&](ir::QueryStage::Drop const&) {
        closed = true;
      },
      [&](ir::QueryStage::Aggregate const&) {
        // A summary does not depend on the order of its input.
        dynamic_key = None{};
        closed = true;
        reads_table = false;
      },
      [&](ir::QueryStage::Limit const&) {
        closed = true;
      },
      [](ir::QueryStage::Filter const&) {});
  }
  return result;
}

// -- ClickHouseDialect --------------------------------------------------------

auto ClickHouseDialect::quote_identifier(std::string_view name) const
  -> std::string {
  return quote_sql_identifier(name);
}

auto ClickHouseDialect::quote_string(std::string_view text) const
  -> Option<std::string> {
  return quote_sql_string(text);
}

auto ClickHouseDialect::render_double(double value) const -> std::string {
  // `{}` renders an integral value without a fraction, which ClickHouse would
  // read as an integer. A trailing `.` keeps it a `Float64`.
  auto result = fmt::format("{}", value);
  if (result.find_first_of(".e") == std::string::npos) {
    result += '.';
  }
  return result;
}

auto ClickHouseDialect::render_time(time value) const -> Option<std::string> {
  // Unlike `toDateTime64` from a string, which saturates before 1900, the
  // nanoseconds since the epoch denote every instant of TQL, in UTC.
  return fmt::format("fromUnixTimestamp64Nano({}, 'UTC')",
                     value.time_since_epoch().count());
}

auto ClickHouseDialect::render_ip(ip value) const -> Option<std::string> {
  return fmt::format("{}({})", value.is_v4() ? "toIPv4" : "toIPv6",
                     quote_sql_string(fmt::to_string(value)));
}

auto ClickHouseDialect::bind_field(std::span<std::string const> path) const
  -> Option<std::string> {
  // ClickHouse has no empty identifiers.
  if (path.empty() or std::ranges::any_of(path, &std::string::empty)) {
    return None{};
  }
  if (bound_) {
    auto it
      = bound_->fields.find(std::vector<std::string>{path.begin(), path.end()});
    if (it == bound_->fields.end()) {
      return None{};
    }
    return it->second.sql;
  }
  // Before the operator knows the table, any path may bind.
  auto result = quote_sql_identifier(path.front());
  for (auto const& segment : path.subspan(1)) {
    result = spell_element(result, segment);
  }
  return result;
}

auto ClickHouseDialect::render_binary(
  ast::binary_expr const& x, pushdown::ExpressionRenderer const& renderer) const
  -> Option<Option<std::string>> {
  auto const default_spelling
    = Option<Option<std::string>>{Option<std::string>{}};
  if (not bound_
      or (x.op != ast::binary_op::eq and x.op != ast::binary_op::neq)) {
    return default_spelling;
  }
  auto const is_null = [](ast::expression const& expr) {
    auto const* constant = try_as<ast::constant>(expr);
    return constant and is<caf::none_t>(constant->value);
  };
  auto const* operand = is_null(x.right)  ? &x.left
                        : is_null(x.left) ? &x.right
                                          : nullptr;
  if (not operand) {
    return default_spelling;
  }
  auto path = ast::field_path::try_from(*operand);
  if (not path) {
    return default_spelling;
  }
  auto segments = std::vector<std::string>{};
  for (auto const& segment : path->path()) {
    segments.push_back(segment.id.name);
  }
  auto it = bound_->fields.find(segments);
  if (it == bound_->fields.end() or not it->second.object) {
    return default_spelling;
  }
  // The field reads the table only where the stage reads it, and not the
  // result of a projection with a field of the same name.
  auto value = renderer.value(*operand);
  if (not value or *value != it->second.sql) {
    return default_spelling;
  }
  // ClickHouse reads an object at the path as `NULL`, but the object below the
  // path has the paths of its values. An empty object has none, as ClickHouse
  // stores no trace of it. A comparison of an object with a literal stays
  // `NULL`, which matches no row.
  auto present = fmt::format("(({} IS NOT NULL) OR notEmpty(JSONAllPaths({})))",
                             it->second.sql, *it->second.object);
  if (x.op == ast::binary_op::neq) {
    return Option<std::string>{std::move(present)};
  }
  return Option<std::string>{fmt::format("(NOT {})", present)};
}

auto ClickHouseDialect::render_call(std::string_view name,
                                    std::span<std::string const> args) const
  -> Option<std::string> {
  struct Function {
    std::string_view tql;
    std::string_view clickhouse;
    size_t arity;
  };
  static constexpr auto functions = std::array{
    Function{"starts_with", "startsWith", 2},
    Function{"ends_with", "endsWith", 2},
    Function{"length_bytes", "length", 1},
    Function{"length_chars", "lengthUTF8", 1},
    Function{"to_lower", "lowerUTF8", 1},
    Function{"to_upper", "upperUTF8", 1},
    Function{"trim", "trimBoth", 1},
    Function{"abs", "abs", 1},
    Function{"match_regex", "match", 2},
  };
  auto it = std::ranges::find(functions, name, &Function::tql);
  if (it == functions.end() or args.size() != it->arity) {
    return None{};
  }
  return fmt::format("{}({})", it->clickhouse, fmt::join(args, ", "));
}

auto ClickHouseDialect::render_aggregate(std::string_view name,
                                         Option<std::string> const& arg) const
  -> Option<std::string> {
  if (name == "count") {
    // TQL counts as `int64`, ClickHouse as `UInt64`.
    return fmt::format("toInt64(count({}))", arg ? *arg : std::string{});
  }
  if (not arg) {
    return None{};
  }
  // The `OrNull` combinator yields `NULL` for no rows, as TQL does, instead of
  // the default value of the type. A sum or a mean would yield `NaN` or an
  // infinity for some input, which the operator reads as `null`.
  if (name == "min" or name == "max") {
    return fmt::format("{}OrNull({})", name, *arg);
  }
  return None{};
}

auto ClickHouseDialect::render_exclude(
  std::span<std::string const> columns) const -> Option<std::string> {
  auto quoted = std::vector<std::string>{};
  for (auto const& column : columns) {
    if (column.empty()) {
      return None{};
    }
    quoted.push_back(quote_sql_identifier(column));
  }
  return fmt::format("* EXCEPT ({})", fmt::join(quoted, ", "));
}

auto absorb_stages(ir::QueryChain const& chain) -> ir::Absorption {
  return pushdown::absorb_prefix(chain, ClickHouseDialect{},
                                 {
                                   .target = "ClickHouse",
                                   .filters_only = false,
                                   .veto = {},
                                 });
}

auto make_table_read(std::string_view table, std::string_view selection,
                     std::span<ir::QueryStage const> plan,
                     std::span<std::string const> columns,
                     BoundPlan const& bound)
  -> Result<pushdown::TableRead, std::string> {
  return pushdown::make_table_read(table, selection, {}, plan, columns,
                                   ClickHouseDialect{bound});
}

} // namespace tenzir::plugins::clickhouse
