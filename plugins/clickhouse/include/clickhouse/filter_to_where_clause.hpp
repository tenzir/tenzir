//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/ir.hpp"

#include <string>
#include <unordered_set>

namespace tenzir::plugins::clickhouse {

struct FilterToWhereClauseResult {
  std::string predicate;
  ir::optimize_filter residual_filter;
};

/// Converts a filter into a ClickHouse WHERE clause.
/// Conjuncts that reference columns not in `known_columns` are moved to
/// `residual_filter` instead of being pushed to SQL.
auto filter_to_where_clause(ir::optimize_filter const& filter,
                            std::unordered_set<std::string> const& known_columns)
  -> FilterToWhereClauseResult;

} // namespace tenzir::plugins::clickhouse
