//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/session.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/tokens.hpp"

#include <string_view>
#include <vector>

namespace tenzir {

auto parse(std::span<token> tokens, std::string_view source, session ctx)
  -> failure_or<ast::pipeline>;

auto parse(std::string_view source, session ctx) -> failure_or<ast::pipeline>;

// TODO: These functions are only a temporary solution.
auto parse_pipeline_with_bad_diagnostics(std::string_view source, session ctx)
  -> failure_or<ast::pipeline>;
auto parse_expression_with_bad_diagnostics(std::string_view source, session ctx)
  -> failure_or<ast::expression>;
struct expression_stream {
  std::vector<ast::expression> expressions;
  size_t bytes_consumed = 0;
  bool has_error = false;
};
auto parse_expression_stream_with_bad_diagnostics(std::string_view source,
                                                  session ctx)
  -> failure_or<expression_stream>;
auto parse_assignment_with_bad_diagnostics(std::string_view source, session ctx)
  -> failure_or<ast::assignment>;
auto parse_multiple_assignments_with_bad_diagnostics(std::string_view source,
                                                     session ctx)
  -> failure_or<std::vector<ast::assignment>>;

} // namespace tenzir
