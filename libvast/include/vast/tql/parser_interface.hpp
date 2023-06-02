//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/pipeline.hpp"
#include "vast/tql/expression.hpp"

namespace vast::tql {

// TODO: I'm not really happy with how this works.
class parser_interface {
public:
  virtual ~parser_interface() = default;

  //
  virtual auto accept_shell_arg() -> std::optional<located<std::string>> = 0;

  /// --long-option or -s (only a single letter is allowed)
  virtual auto accept_long_option() -> std::optional<located<std::string>> = 0;

  /// TODO: What about multiple short options at once (`-abc`)?
  virtual auto accept_short_option() -> std::optional<located<std::string>> = 0;

  //

  virtual auto accept_identifier() -> std::optional<identifier> = 0;

  virtual auto peek_identifier() -> std::optional<identifier> = 0;

  virtual auto accept_equals() -> std::optional<location> = 0;

  virtual auto accept_char(char c) -> std::optional<location> = 0;

  //

  virtual auto parse_operator() -> located<operator_ptr> = 0;

  virtual auto parse_expression() -> expression = 0;

  virtual auto parse_legacy_expression() -> located<vast::expression> = 0;

  virtual auto parse_extractor() -> extractor = 0;

  virtual auto at_end() -> bool = 0;

  virtual auto current_span() -> location = 0;
};

class until_keyword_parser final : public parser_interface {
public:
  explicit until_keyword_parser(std::string keyword, parser_interface& p)
    : keyword_{std::move(keyword)}, p_{p} {
  }

  auto parse_operator() -> located<operator_ptr> override {
    if (at_end()) {
      return {};
    }
    return p_.parse_operator();
  }

  auto accept_identifier() -> std::optional<identifier> override {
    if (at_end()) {
      return {};
    }
    return p_.accept_identifier();
  }

  auto peek_identifier() -> std::optional<identifier> override {
    if (at_end()) {
      return {};
    }
    return p_.peek_identifier();
  }

  auto accept_equals() -> std::optional<location> override {
    if (at_end()) {
      return {};
    }
    return p_.accept_equals();
  }

  auto accept_char(char c) -> std::optional<location> override {
    if (at_end()) {
      return {};
    }
    return p_.accept_char(c);
  }

  auto parse_expression() -> expression override {
    if (at_end()) {
      diagnostic::error("expected expression").primary(current_span()).throw_();
    }
    return p_.parse_expression();
  }

  auto accept_shell_arg() -> std::optional<located<std::string>> override {
    if (at_end()) {
      return {};
    }
    return p_.accept_shell_arg();
  }

  auto parse_legacy_expression() -> located<vast::expression> override {
    if (at_end()) {
      diagnostic::error("expected expression").primary(current_span());
    }
    return p_.parse_legacy_expression();
  }

  auto accept_short_option() -> std::optional<located<std::string>> override {
    if (at_end()) {
      return {};
    }
    return p_.accept_short_option();
  }

  auto accept_long_option() -> std::optional<located<std::string>> override {
    if (at_end()) {
      return {};
    }
    return p_.accept_long_option();
  }

  auto parse_extractor() -> extractor override {
    return p_.parse_extractor();
  }

  auto at_end() -> bool override {
    if (p_.at_end()) {
      return true;
    }
    if (auto word = p_.peek_identifier()) {
      if (word == keyword_) {
        return true;
      }
    }
    return false;
  }

  auto current_span() -> location override {
    return p_.current_span();
  }

private:
  std::string keyword_;
  parser_interface& p_;
};

} // namespace vast::tql
