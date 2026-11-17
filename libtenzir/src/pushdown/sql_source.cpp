//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/sql_source.hpp"

#include "tenzir/detail/string.hpp"
#include "tenzir/tql2/ast.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>

namespace tenzir::pushdown {

auto plan_inputs(std::span<ir::QueryStage const> stages)
  -> Option<std::vector<std::string>> {
  auto result = std::vector<std::string>{};
  auto const add = [&](std::span<std::string const> path) {
    if (not path.empty() and not std::ranges::contains(result, path.front())) {
      result.push_back(path.front());
    }
  };
  auto const add_refs = [&](ast::expression const& expr) {
    auto refs = ast::collect_refs(expr);
    if (not refs) {
      return false;
    }
    for (auto const& path : refs->field_paths) {
      if (path.has_this() or path.path().empty()) {
        return false;
      }
      add(std::vector{std::string{path.path().front().id.name}});
    }
    return true;
  };
  for (auto const& stage : stages) {
    auto known = match(
      stage.kind,
      [&](ir::QueryStage::Filter const& x) {
        return add_refs(x.predicate);
      },
      [&](ir::QueryStage::Select const& x) {
        for (auto const& field : x.fields) {
          add(field.input);
        }
        return true;
      },
      [&](ir::QueryStage::Aggregate const& x) {
        return std::ranges::all_of(x.columns, [&](auto const& column) {
          return std::ranges::all_of(column.args, add_refs);
        });
      },
      [&](ir::QueryStage::Sort const& x) {
        for (auto const& key : x.keys) {
          add(key.field);
        }
        return true;
      },
      [](auto const&) {
        return true;
      });
    if (not known) {
      return None{};
    }
  }
  return result;
}

auto select_projection(Option<ir::OptimizeProjection> const& projection,
                       std::span<std::string const> columns,
                       std::span<std::string const> required,
                       std::span<ir::QueryStage const> stages,
                       SqlQueryDialect const& dialect, SpellColumn const& spell)
  -> std::string {
  // A drop keeps every other column, so a projection below it could leave it
  // nothing to keep. The target prunes the columns of a derived table itself.
  auto const drops = std::ranges::any_of(stages, [](auto const& stage) {
    return is<ir::QueryStage::Drop>(stage.kind);
  });
  if (not projection or drops) {
    return "*";
  }
  auto wanted = std::vector<std::string>{};
  auto const want = [&](std::string const& column) {
    if (not std::ranges::contains(wanted, column)) {
      wanted.push_back(column);
    }
  };
  // Selects the columns that `name` matches, and returns whether there are
  // any.
  auto const resolve = [&](std::string_view name) {
    if (auto exact = std::ranges::find(columns, name); exact != columns.end()) {
      want(*exact);
      return true;
    }
    auto found = false;
    for (auto const& column : columns) {
      if (detail::ascii_icase_equal(column, name)) {
        want(column);
        found = true;
      }
    }
    return found;
  };
  // A target may resolve a name to a column beyond ASCII by rules that the
  // comparison above does not know, such as a collation that folds `ä` to
  // `Ä`, or `cafe` to `café` without regard to accents. If such a name
  // matches no column, the read keeps every column. A name that matches no
  // column while both it and every column are ASCII is left to the operators
  // downstream.
  auto const beyond_ascii = [](std::string_view text) {
    return std::ranges::any_of(text, [](char c) {
      return static_cast<unsigned char>(c) >= 0x80;
    });
  };
  auto const columns_beyond_ascii = std::ranges::any_of(columns, beyond_ascii);
  auto const unknown = [&](std::string_view name) {
    return not resolve(name) and (columns_beyond_ascii or beyond_ascii(name));
  };
  for (auto const& path : *projection) {
    if (not path.path().empty() and unknown(path.path().front().id.name)) {
      return "*";
    }
  }
  for (auto const& name : required) {
    if (unknown(name)) {
      return "*";
    }
  }
  if (wanted.empty()) {
    // Downstream needs no field, but it needs the rows.
    if (columns.empty()) {
      return "*";
    }
    wanted.push_back(columns.front());
  }
  auto spelled = std::vector<std::string>{};
  for (auto const& column : wanted) {
    spelled.push_back(spell ? spell(column) : dialect.quote_identifier(column));
  }
  return fmt::to_string(fmt::join(spelled, ", "));
}

auto plan_table_read(std::string_view target, SqlQueryDialect const& dialect,
                     SqlMode mode, std::span<ir::QueryStage const> stages,
                     SqlTable const* table) -> Result<TableRead, std::string> {
  if (mode == SqlMode::query and not stages.empty()) {
    return Err{std::string{"the query of the user is opaque"}};
  }
  if (mode == SqlMode::live
      and not std::ranges::all_of(stages, [](auto const& stage) {
            return is<ir::QueryStage::Filter>(stage.kind);
          })) {
    return Err{std::string{"a live read takes over filters only"}};
  }
  auto const declined = fmt::format("{} cannot spell the stage", target);
  // At plan time, the read spells the stages for a placeholder table.
  auto const placeholder = SqlTable{.from = "t"};
  auto const& t = table ? *table : placeholder;
  auto base = SqlSelect{
    .selection = t.selection,
    .from = t.from,
    .where = {},
    .group_by = {},
    .order_by = {},
    .limit = None{},
  };
  if (t.window or (dialect.filters_in_projection() and t.selection != "*")) {
    // The filters that directly follow the source read the table, so they
    // may join the projection of it, which then constrains the table itself.
    auto const leading = std::ranges::find_if(stages, [](auto const& stage) {
      return not is<ir::QueryStage::Filter>(stage.kind);
    });
    auto const count = static_cast<size_t>(leading - stages.begin());
    if (count > 0) {
      auto rendered = render_sql(
        SqlRelation{.from = t.from, .fields = None{}, .alias = None{}},
        stages.first(count), dialect);
      if (not rendered or rendered->select.from != t.from
          or rendered->select.selection != "*") {
        return Err{declined};
      }
      base.where = std::move(rendered->select.where);
      stages = stages.subspan(count);
    }
  }
  if (t.window) {
    // The bounds come first, so that the target can prune by them.
    base.where.insert(base.where.begin(), t.window->bounds.begin(),
                      t.window->bounds.end());
    base.order_by
      = {fmt::format("{} ASC", dialect.quote_identifier(t.window->tracking))};
  }
  if (stages.empty()) {
    return TableRead{
      .sql = render_select(base, dialect),
      .layout = {.fields = None{}, .hidden = {}, .schema_name = None{}},
    };
  }
  // The stages read the table, or the projection of it that downstream needs.
  auto relation
    = SqlRelation{.from = t.from, .fields = None{}, .alias = None{}};
  if (t.selection != "*" or not base.where.empty()) {
    auto const name = std::string{"__tenzir_table"};
    relation.from = fmt::format("({}) AS {}", render_select(base, dialect),
                                dialect.quote_identifier(name));
    relation.alias = name;
  }
  auto rendered = render_sql(std::move(relation), stages, dialect);
  if (not rendered) {
    return Err{declined};
  }
  for (auto const& alias : rendered->aliases) {
    if (std::ranges::any_of(t.columns, [&](std::string const& column) {
          return detail::ascii_icase_equal(column, alias);
        })) {
      return Err{fmt::format("the table has a column named `{}`, which the "
                             "query needs for itself",
                             alias)};
    }
  }
  return TableRead{
    .sql = render_select(rendered->select, dialect),
    .layout = std::move(rendered->layout),
  };
}

auto sql_planner(std::string target, SqlQueryDialect const& dialect,
                 SqlMode mode) -> Planner<TableRead, SqlTable> {
  return {
    .target = target,
    .plan =
      [target, &dialect, mode](std::span<ir::QueryStage const> stages,
                               SqlTable const* table) {
        return plan_table_read(target, dialect, mode, stages, table);
      },
  };
}

} // namespace tenzir::pushdown
