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

auto parse(std::span<token> tokens, Source const& source, session ctx)
  -> failure_or<ast::pipeline>;

auto parse(Source const& source, session ctx) -> failure_or<ast::pipeline>;

/// Parse a substring with an explicit source origin. This allows parsing a
/// substring of a larger file while keeping locations correct relative to the
/// full source (using `location_offset`) or overriding all locations to a fixed
/// value (using `location`).
auto parse(std::string_view source, source_origin origin, session ctx)
  -> failure_or<ast::pipeline>;

/// Parse a source with an explicit location override. This is useful when
/// parsing ad-hoc TQL. When passed a `location`, all locations in the resulting
/// AST will point to that location. When passed a `location_offset`, token
/// positions are shifted by the offset and use the given source id.
auto parse_pipeline_with_location_override(std::string_view source,
                                           source_origin origin, session ctx)
  -> failure_or<ast::pipeline>;

auto parse_expression_with_location_override(std::string_view source,
                                             source_origin origin, session ctx)
  -> failure_or<ast::expression>;

auto parse_type_def_with_location_override(std::string_view source,
                                           source_origin origin, session ctx)
  -> failure_or<ast::type_def>;

struct expression_stream {
  std::vector<ast::expression> expressions;
  size_t bytes_consumed = 0;
  bool has_error = false;
};

auto parse_expression_stream_with_location_override(std::string_view source,
                                                    source_origin origin,
                                                    session ctx)
  -> failure_or<expression_stream>;

auto parse_assignment_with_location_override(std::string_view source,
                                             source_origin origin, session ctx)
  -> failure_or<ast::assignment>;

auto parse_multiple_assignments_with_location_override(std::string_view source,
                                                       source_origin origin,
                                                       session ctx)
  -> failure_or<std::vector<ast::assignment>>;

} // namespace tenzir
