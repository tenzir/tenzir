//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/tql2/ast.hpp"
#include "tenzir/tql2/tokens.hpp"

namespace tenzir {

/// Hybrid formatter that combines AST structure with token-based comment preservation.
/// This provides comprehensive pretty-printing while preserving all comments.
auto format_hybrid(std::string_view source, const std::vector<token>& tokens, const ast::pipeline& pipeline) -> std::string;

/// Token-based formatter that preserves comments and whitespace (simple approach).
/// This is kept for reference but the hybrid formatter is preferred.
auto format_tokens(std::string_view source, const std::vector<token>& tokens) -> std::string;

/// Legacy AST-based formatter (deprecated - cannot preserve comments).
/// This formatter is fundamentally limited because ASTs don't include comments.
auto format_pipeline(const ast::pipeline& pipeline) -> std::string;

} // namespace tenzir
