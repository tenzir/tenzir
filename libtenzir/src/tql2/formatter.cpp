//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/formatter.hpp"

#include "tenzir/session.hpp"
#include "tenzir/tql2/tokens.hpp"

#include <sstream>
#include <stack>
#include <string>
#include <string_view>
#include <unordered_set>

namespace tenzir {

namespace {

/// Context for tracking formatting state during token processing.
class format_context {
public:
  explicit format_context(const format_config& config)
    : config_{config} {}

  /// Current indentation level.
  auto indent_level() const -> size_t {
    return indent_level_;
  }

  /// Increase indentation by one level.
  auto indent() -> void {
    ++indent_level_;
  }

  /// Decrease indentation by one level.
  auto dedent() -> void {
    if (indent_level_ > 0) {
      --indent_level_;
    }
  }

  /// Get current indentation string.
  auto current_indent() const -> std::string {
    return std::string(indent_level_ * config_.indent_size, ' ');
  }

  /// Check if we're at the start of a line.
  auto at_line_start() const -> bool {
    return at_line_start_;
  }

  /// Mark that we're no longer at line start.
  auto mark_content() -> void {
    at_line_start_ = false;
  }

  /// Mark that we're at the start of a new line.
  auto mark_line_start() -> void {
    at_line_start_ = true;
  }

  /// Get the formatting configuration.
  auto config() const -> const format_config& {
    return config_;
  }

  /// Check if we're inside a string literal.
  auto in_string() const -> bool {
    return in_string_;
  }

  /// Set string state.
  auto set_in_string(bool value) -> void {
    in_string_ = value;
  }

  /// Track nesting depth for braces/brackets/parentheses.
  auto push_nesting() -> void {
    ++nesting_depth_;
  }

  auto pop_nesting() -> void {
    if (nesting_depth_ > 0) {
      --nesting_depth_;
    }
  }

  auto nesting_depth() const -> size_t {
    return nesting_depth_;
  }

private:
  const format_config& config_;
  size_t indent_level_ = 0;
  size_t nesting_depth_ = 0;
  bool at_line_start_ = true;
  bool in_string_ = false;
};

/// Helper class to build formatted output efficiently.
class output_builder {
public:
  explicit output_builder(format_context& ctx) : ctx_{ctx} {}

  /// Add text to output.
  auto add(std::string_view text) -> void {
    if (ctx_.at_line_start() && !text.empty() && text != "\n") {
      output_ << ctx_.current_indent();
      ctx_.mark_content();
    }
    output_ << text;
  }

  /// Add a newline and mark line start.
  auto newline() -> void {
    output_ << '\n';
    ctx_.mark_line_start();
  }

  /// Add a space if not at line start.
  auto space() -> void {
    if (!ctx_.at_line_start()) {
      output_ << ' ';
    }
  }

  /// Get the final formatted string.
  auto str() -> std::string {
    return output_.str();
  }

private:
  std::ostringstream output_;
  format_context& ctx_;
};

/// Check if a token kind represents a binary operator that needs spacing.
auto needs_operator_spacing(token_kind kind) -> bool {
  using enum token_kind;
  switch (kind) {
    case plus:
    case minus:
    case star:
    case slash:
    case equal_equal:
    case bang_equal:
    case less:
    case less_equal:
    case greater:
    case greater_equal:
    case and_:
    case or_:
    case in:
    case equal:
      return true;
    default:
      return false;
  }
}

/// Check if a token needs space before it.
auto needs_space_before_token(token_kind kind) -> bool {
  using enum token_kind;
  return kind == else_;
}

/// Check if a token content represents a keyword that needs space after.
auto needs_keyword_spacing(token_kind kind, std::string_view content) -> bool {
  using enum token_kind;
  
  // Handle special keyword tokens
  switch (kind) {
    case if_:
    case else_:
    case let:
    case not_:
    case move:
      return true;
    case identifier: {
      // TQL operators that need spacing (based on operators.mdx documentation)
      static const std::unordered_set<std::string_view> tql_operators = {
        "api", "batch", "buffer", "cache", "legacy", "local", "measure", "remote", 
        "serve", "strict", "unordered", "assert", "assert_throughput", "deduplicate",
        "head", "sample", "slice", "tail", "taste", "where", "compress", "decompress",
        "cron", "delay", "discard", "every", "fork", "load_balance", "pass", "repeat",
        "throttle", "diagnostics", "metrics", "openapi", "plugins", "version", "drop",
        "enumerate", "http", "move", "select", "set", "timeshift", "unroll", "export", 
        "fields", "import", "partitions", "schemas", "files", "nics", "processes", 
        "sockets", "from", "python", "shell", "rare", "reverse", "sort", "summarize", 
        "top", "to", "sigma", "yara"
      };
      return tql_operators.contains(content);
    }
    default:
      return false;
  }
}

/// Check if a token kind opens a nesting context.
auto opens_nesting(token_kind kind) -> bool {
  using enum token_kind;
  return kind == lbrace || kind == lbracket || kind == lpar;
}

/// Check if a token kind closes a nesting context.
auto closes_nesting(token_kind kind) -> bool {
  using enum token_kind;
  return kind == rbrace || kind == rbracket || kind == rpar;
}

/// Check if we should break the line before this token.
auto should_break_before(token_kind kind, const format_context& ctx) -> bool {
  using enum token_kind;
  
  // Always break before closing braces at nesting depth > 0
  if (kind == rbrace && ctx.nesting_depth() > 0) {
    return true;
  }
  
  // Break before else if not compact
  if (kind == else_ && !ctx.config().compact) {
    return false; // else should be on same line as closing brace
  }
  
  return false;
}

/// Check if we should break the line after this token.
auto should_break_after(token_kind kind, const format_context& /* ctx */) -> bool {
  using enum token_kind;
  
  // Always break after opening braces
  if (kind == lbrace) {
    return true;
  }
  
  // Pipes are handled separately in the main loop
  
  // Break after semicolon-like separators
  return false;
}

/// Check if this token should cause indentation for following content
auto should_indent_after(token_kind kind) -> bool {
  using enum token_kind;
  return kind == lbrace;
}

/// Check if this token should cause dedentation
auto should_dedent_before(token_kind kind) -> bool {
  using enum token_kind;
  return kind == rbrace;
}

/// Get the token content from the source.
auto get_token_content(const token& tok, std::string_view source, 
                       size_t prev_end) -> std::string_view {
  if (prev_end > source.length() || tok.end > source.length()) {
    return {};
  }
  return source.substr(prev_end, tok.end - prev_end);
}

} // namespace

auto format_tokens(std::span<const token> tokens, std::string_view source,
                   const format_config& config) -> std::string {
  auto ctx = format_context{config};
  auto builder = output_builder{ctx};
  
  size_t pos = 0;
  bool prev_was_operator = false;
  bool needs_space_before = false;
  bool prev_added_space = false;
  
  for (size_t i = 0; i < tokens.size(); ++i) {
    const auto& tok = tokens[i];
    auto content = get_token_content(tok, source, pos);
    
    // Handle whitespace and comments specially
    if (tok.kind == token_kind::whitespace) {
      pos = tok.end;
      continue; // Skip original whitespace, we control formatting
    }
    
    if (tok.kind == token_kind::line_comment || tok.kind == token_kind::delim_comment) {
      // Preserve comments with proper spacing
      if (!ctx.at_line_start()) {
        builder.space();
      }
      builder.add(content);
      if (tok.kind == token_kind::line_comment) {
        builder.newline();
      }
      pos = tok.end;
      continue;
    }
    
    // Handle string content (preserve as-is when inside strings)
    if (ctx.in_string()) {
      builder.add(content);
      if (tok.kind == token_kind::closing_quote) {
        ctx.set_in_string(false);
      }
      pos = tok.end;
      continue;
    }
    
    // Handle string beginnings
    if (tok.kind == token_kind::string_begin || 
        tok.kind == token_kind::raw_string_begin ||
        tok.kind == token_kind::blob_begin ||
        tok.kind == token_kind::raw_blob_begin ||
        tok.kind == token_kind::format_string_begin) {
      ctx.set_in_string(true);
    }
    
    // Check if we need a line break before this token
    if (should_break_before(tok.kind, ctx)) {
      if (!ctx.at_line_start()) {
        builder.newline();
      }
    }
    
    // Handle dedentation before closing braces
    if (should_dedent_before(tok.kind)) {
      ctx.dedent();
      if (!ctx.at_line_start()) {
        builder.newline();
      }
    }
    
    // Handle nesting for bracket/paren tracking
    if (opens_nesting(tok.kind)) {
      ctx.push_nesting();
    }
    
    if (closes_nesting(tok.kind)) {
      ctx.pop_nesting();
    }
    
    // Add space before token if needed
    if (needs_space_before || 
        (needs_operator_spacing(tok.kind) && !ctx.at_line_start() && !prev_was_operator) ||
        (tok.kind == token_kind::lbrace && !ctx.at_line_start() && !prev_added_space) ||
        (tok.kind == token_kind::lbracket && !ctx.at_line_start() && !prev_added_space) ||
        (needs_space_before_token(tok.kind) && !ctx.at_line_start())) {
      builder.space();
    }
    
    // Convert pipes to newlines in pipelines
    if (tok.kind == token_kind::pipe) {
      builder.newline();
    } else {
      builder.add(content);
    }
    
    // Handle spacing after token
    bool needs_space_after = false;
    
    if (needs_operator_spacing(tok.kind) || needs_keyword_spacing(tok.kind, content)) {
      needs_space_after = true;
    }
    
    // Always add space after comma and colon
    if (tok.kind == token_kind::comma || tok.kind == token_kind::colon) {
      needs_space_after = true;
    }
    
    // Check if next token should not have space before it
    bool next_forbids_space = false;
    if (i + 1 < tokens.size()) {
      auto next_kind = tokens[i + 1].kind;
      next_forbids_space = (next_kind == token_kind::dot || 
                           next_kind == token_kind::lpar ||
                           next_kind == token_kind::comma);
    }
    
    if (needs_space_after && !next_forbids_space) {
      builder.space();
      prev_added_space = true;
    } else {
      prev_added_space = false;
    }
    
    // Handle indentation BEFORE line break for opening braces
    if (should_indent_after(tok.kind)) {
      ctx.indent();
    }
    
    // Check if we need a line break after this token
    if (should_break_after(tok.kind, ctx)) {
      builder.newline();
    }
    
    // Update state for next iteration
    prev_was_operator = needs_operator_spacing(tok.kind);
    needs_space_before = false;
    
    pos = tok.end;
  }
  
  return builder.str();
}

auto format_tql(std::string_view source, const format_config& config,
                session ctx) -> failure_or<std::string> {
  TRY(auto tokens, tokenize(source, ctx));
  return format_tokens(tokens, source, config);
}

} // namespace tenzir