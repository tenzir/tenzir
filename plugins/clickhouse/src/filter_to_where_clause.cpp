//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "clickhouse/filter_to_where_clause.hpp"

#include "clickhouse/arguments.hpp"
#include "tenzir/data.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

#include <algorithm>
#include <vector>

namespace tenzir::plugins::clickhouse {

namespace {

auto escape_sql_string(std::string_view value) -> std::string {
  auto result = std::string{};
  result.reserve(value.size() + 2);
  result.push_back('\'');
  for (auto c : value) {
    if (c == '\'') {
      result.append("''");
    } else if (c == '\\') {
      result.append("\\\\");
    } else {
      result.push_back(c);
    }
  }
  result.push_back('\'');
  return result;
}

auto to_clickhouse_literal(data const& value) -> Option<std::string> {
  return match(
    value.get_data(),
    [](caf::none_t) -> Option<std::string> {
      return std::string{"NULL"};
    },
    [](bool value) -> Option<std::string> {
      return std::string{value ? "true" : "false"};
    },
    [](int64_t value) -> Option<std::string> {
      return fmt::to_string(value);
    },
    [](uint64_t value) -> Option<std::string> {
      return fmt::to_string(value);
    },
    [](double value) -> Option<std::string> {
      return fmt::to_string(value);
    },
    [](std::string const& value) -> Option<std::string> {
      return escape_sql_string(value);
    },
    [](auto const&) -> Option<std::string> {
      return None{};
    });
}

auto to_clickhouse_identifier(ast::field_path const& path) -> std::string {
  auto result = std::vector<std::string>{};
  for (auto const& segment : path.path()) {
    result.push_back(quote_identifier_component(segment.id.name));
  }
  return fmt::to_string(fmt::join(result, "."));
}

auto to_clickhouse_sql(ast::expression const& expr) -> Option<std::string>;

auto to_clickhouse_binary_op(ast::binary_op op) -> Option<std::string_view> {
  switch (op) {
    using enum ast::binary_op;
    case eq:
      return std::string_view{"="};
    case neq:
      return std::string_view{"!="};
    case gt:
      return std::string_view{">"};
    case geq:
      return std::string_view{">="};
    case lt:
      return std::string_view{"<"};
    case leq:
      return std::string_view{"<="};
    case and_:
      return std::string_view{"AND"};
    case or_:
      return std::string_view{"OR"};
    default:
      return None{};
  }
}

auto to_clickhouse_sql(ast::expression const& expr) -> Option<std::string> {
  if (auto path = ast::field_path::try_from(expr)) {
    auto has_optional_access
      = std::ranges::any_of(path->path(), [](auto const& segment) {
          return segment.has_question_mark;
        });
    if (path->has_this() or path->path().empty() or has_optional_access) {
      return None{};
    }
    return to_clickhouse_identifier(*path);
  }
  return expr.match(
    [](ast::constant const& constant) -> Option<std::string> {
      return to_clickhouse_literal(constant.as_data());
    },
    [](ast::binary_expr const& binary) -> Option<std::string> {
      auto lhs = to_clickhouse_sql(binary.left);
      auto op = to_clickhouse_binary_op(binary.op.inner);
      auto rhs = to_clickhouse_sql(binary.right);
      if (not lhs or not op or not rhs) {
        return None{};
      }
      return fmt::format("({} {} {})", *lhs, *op, *rhs);
    },
    [](auto const&) -> Option<std::string> {
      return None{};
    });
}

auto collect_conjuncts(ast::expression const& expr,
                       std::vector<ast::expression const*>& result) -> void {
  auto const* binary = try_as<ast::binary_expr>(expr);
  if (binary == nullptr or binary->op.inner != ast::binary_op::and_) {
    result.push_back(&expr);
    return;
  }
  collect_conjuncts(binary->left, result);
  collect_conjuncts(binary->right, result);
}

} // namespace

auto filter_to_where_clause(ir::optimize_filter const& filter)
  -> FilterToWhereClauseResult {
  auto clauses = std::vector<std::string>{};
  auto residual_filter = ir::optimize_filter{};
  for (auto const& expr : filter) {
    auto conjuncts = std::vector<ast::expression const*>{};
    collect_conjuncts(expr, conjuncts);
    for (auto const* conjunct : conjuncts) {
      if (auto sql = to_clickhouse_sql(*conjunct)) {
        clauses.push_back(std::move(*sql));
      } else {
        residual_filter.push_back(*conjunct);
      }
    }
  }
  if (clauses.empty()) {
    return {
      .sql = {},
      .residual_filter = std::move(residual_filter),
    };
  }
  return {
    .sql = fmt::format(" WHERE {}", fmt::join(clauses, " AND ")),
    .residual_filter = std::move(residual_filter),
  };
}

} // namespace tenzir::plugins::clickhouse
