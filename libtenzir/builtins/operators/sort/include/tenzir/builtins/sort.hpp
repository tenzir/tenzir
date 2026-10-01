//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/async/fwd.hpp"
#include "tenzir/location.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

#include <cstdint>
#include <vector>

namespace tenzir::plugins::sort {

struct SortArgs {
  std::vector<ast::expression> exprs;
  location keyword;
};

auto make_sort(SortArgs args, Option<uint64_t> limit) -> AnyOperator;

namespace legacy {

auto make_sort(SortArgs args) -> AnyOperator;

} // namespace legacy

} // namespace tenzir::plugins::sort
