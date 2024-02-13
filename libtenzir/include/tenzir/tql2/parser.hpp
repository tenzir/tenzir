//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/diagnostics.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/tokens.hpp"

namespace tenzir::tql2 {

// TODO
auto parse(std::span<token> tokens, std::string_view source,
           diagnostic_handler& diag) -> ast::pipeline;

} // namespace tenzir::tql2
