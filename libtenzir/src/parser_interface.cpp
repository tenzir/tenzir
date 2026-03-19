//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/parser_interface.hpp"

#include "tenzir/expression.hpp"
#include "tenzir/pipeline.hpp"

namespace tenzir {

auto until_keyword_parser::parse_operator() -> located<operator_ptr> {
  if (at_end()) {
    return {};
  }
  return p_.parse_operator();
}

auto until_keyword_parser::accept_identifier() -> std::optional<identifier> {
  if (at_end()) {
    return {};
  }
  return p_.accept_identifier();
}

auto until_keyword_parser::peek_identifier() -> std::optional<identifier> {
  if (at_end()) {
    return {};
  }
  return p_.peek_identifier();
}

auto until_keyword_parser::accept_equals() -> std::optional<location> {
  if (at_end()) {
    return {};
  }
  return p_.accept_equals();
}

auto until_keyword_parser::accept_char(char c) -> std::optional<location> {
  if (at_end()) {
    return {};
  }
  return p_.accept_char(c);
}

auto until_keyword_parser::peek_char(char c) -> std::optional<location> {
  if (at_end()) {
    return {};
  }
  return p_.peek_char(c);
}

auto until_keyword_parser::parse_expression() -> tql::expression {
  if (at_end()) {
    diagnostic::error("expected expression").primary(current_span()).throw_();
  }
  return p_.parse_expression();
}

auto until_keyword_parser::accept_shell_arg()
  -> std::optional<located<std::string>> {
  if (at_end()) {
    return {};
  }
  return p_.accept_shell_arg();
}

auto until_keyword_parser::peek_shell_arg()
  -> std::optional<located<std::string>> {
  if (at_end()) {
    return {};
  }
  return p_.peek_shell_arg();
}

auto until_keyword_parser::parse_legacy_expression()
  -> located<tenzir::expression> {
  if (at_end()) {
    diagnostic::error("expected expression").primary(current_span()).throw_();
  }
  return p_.parse_legacy_expression();
}

auto until_keyword_parser::parse_extractor() -> tql::extractor {
  return p_.parse_extractor();
}

auto until_keyword_parser::parse_data() -> located<tenzir::data> {
  if (at_end()) {
    diagnostic::error("expected data").primary(current_span()).throw_();
  }
  return p_.parse_data();
}

auto until_keyword_parser::parse_int() -> located<int64_t> {
  if (at_end()) {
    diagnostic::error("expected int64").primary(current_span()).throw_();
  }
  return p_.parse_int();
}

auto until_keyword_parser::at_end() -> bool {
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

auto until_keyword_parser::current_span() -> location {
  return p_.current_span();
}

} // namespace tenzir
