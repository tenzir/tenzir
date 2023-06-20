//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/diagnostics.hpp"
#include "vast/expression.hpp"
#include "vast/tql/expression.hpp"

namespace vast {

/// The interface used for parsing operators and other plugins.
///
/// @warning This interface should be considered very unstable. There are many
/// improvements to be done here.
class parser_interface {
public:
  virtual ~parser_interface() = default;

  virtual auto accept_shell_arg() -> std::optional<located<std::string>> = 0;

  virtual auto peek_shell_arg() -> std::optional<located<std::string>> = 0;

  virtual auto accept_identifier() -> std::optional<identifier> = 0;

  virtual auto peek_identifier() -> std::optional<identifier> = 0;

  virtual auto accept_equals() -> std::optional<location> = 0;

  virtual auto accept_char(char c) -> std::optional<location> = 0;

  virtual auto parse_operator() -> located<operator_ptr> = 0;

  virtual auto parse_expression() -> tql::expression = 0;

  virtual auto parse_legacy_expression() -> located<expression> = 0;

  virtual auto parse_extractor() -> tql::extractor = 0;

  virtual auto at_end() -> bool = 0;

  virtual auto current_span() -> location = 0;
};

/// Wraps another `parser_interface`, but stops at a given keyword.
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

  auto parse_expression() -> tql::expression override {
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

  auto peek_shell_arg() -> std::optional<located<std::string>> override {
    if (at_end()) {
      return {};
    }
    return p_.peek_shell_arg();
  }

  auto parse_legacy_expression() -> located<vast::expression> override {
    if (at_end()) {
      diagnostic::error("expected expression").primary(current_span()).throw_();
    }
    return p_.parse_legacy_expression();
  }

  auto parse_extractor() -> tql::extractor override {
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

} // namespace vast
