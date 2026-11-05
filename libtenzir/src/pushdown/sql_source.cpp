//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/sql_source.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/try.hpp"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>

namespace tenzir::pushdown {

auto final_limit(std::span<ir::QueryStage const> plan) -> Option<uint64_t> {
  if (plan.empty()) {
    return None{};
  }
  auto const* limit = try_as<ir::QueryStage::Limit>(plan.back().kind);
  return limit ? Option<uint64_t>{limit->count} : None{};
}

auto computes_with_time(ast::expression const& expr) -> bool {
  auto const is_time = [](ast::expression const& x) {
    auto const* constant = try_as<ast::constant>(x);
    return constant and is<time>(constant->value);
  };
  if (auto const* x = try_as<ast::binary_expr>(expr)) {
    using enum ast::binary_op;
    auto const arithmetic
      = x->op == add or x->op == sub or x->op == mul or x->op == div;
    if (arithmetic and (is_time(x->left) or is_time(x->right))) {
      return true;
    }
    return computes_with_time(x->left) or computes_with_time(x->right);
  }
  if (auto const* x = try_as<ast::unary_expr>(expr)) {
    return is_time(x->expr) or computes_with_time(x->expr);
  }
  if (auto const* x = try_as<ast::function_call>(expr)) {
    return std::ranges::any_of(x->args, [](ast::expression const& arg) {
      return computes_with_time(arg);
    });
  }
  return false;
}

auto absorb_prefix(ir::QueryChain const& chain, SpellsStages const& spells,
                   AbsorbOptions const& options) -> ir::Absorption {
  if (chain.stages.empty()) {
    return {.prefix = 0, .declined = ""};
  }
  // The stages up to the first one that the source must not take over,
  // whatever the dialect spells.
  auto candidates = chain.stages.size();
  auto reason = std::string{};
  for (auto i = size_t{0}; i < chain.stages.size(); ++i) {
    auto const& stage = chain.stages[i];
    if (options.filters_only and not is<ir::QueryStage::Filter>(stage.kind)) {
      candidates = i;
      reason = "a live read takes over filters only";
      break;
    }
    if (options.veto) {
      if (auto vetoed = options.veto(stage)) {
        candidates = i;
        reason = std::move(*vetoed);
        break;
      }
    }
  }
  auto const declined
    = fmt::format("{} cannot spell the stage", options.target);
  for (auto n = candidates; n > 0; --n) {
    if (spells(std::span{chain.stages}.first(n))) {
      if (n == chain.stages.size()) {
        return {.prefix = n, .declined = ""};
      }
      return {.prefix = n, .declined = n == candidates ? reason : declined};
    }
  }
  return {.prefix = 0, .declined = candidates == 0 ? reason : declined};
}

auto absorb_prefix(ir::QueryChain const& chain, SqlQueryDialect const& dialect,
                   AbsorbOptions const& options) -> ir::Absorption {
  return absorb_prefix(
    chain,
    [&](std::span<ir::QueryStage const> stages) {
      auto relation
        = SqlRelation{.from = "t", .fields = None{}, .alias = None{}};
      return render_plan(std::move(relation), stages, dialect).is_some();
    },
    options);
}

auto plan_inputs(std::span<ir::QueryStage const> plan)
  -> Option<std::vector<std::string>> {
  auto result = std::vector<std::string>{};
  auto const add = [&](std::span<std::string const> path) {
    if (not path.empty() and not std::ranges::contains(result, path.front())) {
      result.push_back(path.front());
    }
  };
  for (auto const& stage : plan) {
    auto known = match(
      stage.kind,
      [&](ir::QueryStage::Filter const& x) {
        auto refs = ast::collect_refs(x.predicate);
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
      },
      [&](ir::QueryStage::Select const& x) {
        for (auto const& field : x.fields) {
          add(field.input);
        }
        return true;
      },
      [&](ir::QueryStage::Aggregate const& x) {
        for (auto const& column : x.columns) {
          if (column.input) {
            add(*column.input);
          }
        }
        return true;
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

auto identifiers_may_collide(std::string_view lhs, std::string_view rhs)
  -> bool {
  return detail::ascii_icase_equal(lhs, rhs);
}

auto select_projection(Option<ir::OptimizeProjection> const& projection,
                       std::span<std::string const> columns,
                       std::span<std::string const> required,
                       std::span<ir::QueryStage const> plan,
                       SqlQueryDialect const& dialect, SpellColumn const& spell)
  -> std::string {
  // A drop keeps every other column, so a projection below it could leave it
  // nothing to keep. The target prunes the columns of a derived table itself.
  auto const drops = std::ranges::any_of(plan, [](auto const& stage) {
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

auto make_table_read(std::string_view table, std::string_view selection,
                     std::span<std::string const> where,
                     std::span<ir::QueryStage const> plan,
                     std::span<std::string const> columns,
                     SqlQueryDialect const& dialect)
  -> Result<TableRead, std::string> {
  auto base = SqlSelect{
    .selection = std::string{selection},
    .from = std::string{table},
    .where = {where.begin(), where.end()},
    .group_by = {},
    .order_by = {},
    .limit = None{},
  };
  if (plan.empty()) {
    return TableRead{
      .sql = render_select(base, dialect),
      .layout = {.fields = None{}, .hidden = {}, .schema_name = None{}},
    };
  }
  // The plan reads the table, or the projection of it that downstream needs.
  auto relation = SqlRelation{
    .from = std::string{table},
    .fields = None{},
    .alias = None{},
  };
  if (selection != "*" or not where.empty()) {
    auto const name = std::string{"__tenzir_table"};
    relation.from = fmt::format("({}) AS {}", render_select(base, dialect),
                                dialect.quote_identifier(name));
    relation.alias = name;
  }
  auto rendered = render_plan(std::move(relation), plan, dialect);
  if (not rendered) {
    return Err{std::string{"failed to spell the stages that the operator took "
                           "over"}};
  }
  for (auto const& alias : rendered->aliases) {
    if (std::ranges::any_of(columns, [&](std::string const& column) {
          return identifiers_may_collide(column, alias);
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

auto make_window_read(std::string_view table, std::string_view selection,
                      std::span<std::string const> bounds,
                      std::span<ir::QueryStage const> filters,
                      std::string_view tracking, SqlQueryDialect const& dialect)
  -> Result<std::string, std::string> {
  auto select = SqlSelect{
    .selection = "*",
    .from = std::string{table},
    .where = {},
    .group_by = {},
    .order_by = {},
    .limit = None{},
  };
  if (not filters.empty()) {
    TENZIR_ASSERT(std::ranges::all_of(filters, [](auto const& stage) {
      return is<ir::QueryStage::Filter>(stage.kind);
    }));
    auto rendered = render_plan(SqlRelation{.from = std::string{table},
                                            .fields = None{},
                                            .alias = None{}},
                                filters, dialect);
    if (not rendered or rendered->select.from != table
        or rendered->select.selection != "*") {
      return Err{std::string{"failed to spell the filters that the operator "
                             "took over"}};
    }
    select = std::move(rendered->select);
  }
  select.selection = std::string{selection};
  // The bounds come first, so that the target can prune by them.
  select.where.insert(select.where.begin(), bounds.begin(), bounds.end());
  select.order_by = {fmt::format("{} ASC", dialect.quote_identifier(tracking))};
  return render_select(select, dialect);
}

} // namespace tenzir::pushdown
