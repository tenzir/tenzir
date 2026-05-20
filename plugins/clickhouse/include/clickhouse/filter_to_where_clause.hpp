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

namespace tenzir::plugins::clickhouse {

struct FilterToWhereClauseResult {
  std::string sql;
  ir::optimize_filter residual_filter;
};

auto filter_to_where_clause(ir::optimize_filter const& filter)
  -> FilterToWhereClauseResult;

} // namespace tenzir::plugins::clickhouse
