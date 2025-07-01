//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/session.hpp"
#include "tenzir/tql2/tokens.hpp"

#include <string>
#include <string_view>

namespace tenzir {

/// Configuration for TQL formatting behavior.
struct format_config {
  /// Number of spaces per indentation level.
  size_t indent_size = 2;

  /// Whether to insert blank lines between major sections.
  bool blank_lines = false;

  /// Maximum line length before wrapping (0 = no limit).
  size_t max_line_length = 80;

  /// Whether to format compactly or with expanded spacing.
  bool compact = false;
};

/// Formats TQL source code using the tokenizer to preserve comments.
///
/// @param source The input TQL source code to format
/// @param config Formatting configuration options
/// @param ctx Session context for error reporting
/// @return The formatted TQL source code or an error
auto format_tql(std::string_view source, const format_config& config,
                session ctx) -> failure_or<std::string>;

/// Formats already tokenized TQL source code.
///
/// @param tokens The tokenized source
/// @param source The original source string (for token content extraction)
/// @param config Formatting configuration options
/// @return The formatted TQL source code
auto format_tokens(std::span<const token> tokens, std::string_view source,
                   const format_config& config) -> std::string;

} // namespace tenzir