//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/prepend_token.hpp"

#include "tenzir/expression.hpp"
#include "tenzir/pipeline.hpp"

namespace tenzir {

auto prepend_token::accept_shell_arg() -> std::optional<located<std::string>> {
  if (token_) {
    auto tmp = make_located_string();
    token_ = std::nullopt;
    return tmp;
  }
  return next_.accept_shell_arg();
}

auto prepend_token::peek_shell_arg() -> std::optional<located<std::string>> {
  if (token_) {
    return make_located_string();
  }
  return next_.peek_shell_arg();
}

auto prepend_token::accept_identifier() -> std::optional<identifier> {
  TENZIR_ASSERT(not token_);
  return next_.accept_identifier();
}

auto prepend_token::peek_identifier() -> std::optional<identifier> {
  TENZIR_ASSERT(not token_);
  return next_.peek_identifier();
}

auto prepend_token::accept_equals() -> std::optional<location> {
  TENZIR_ASSERT(not token_);
  return next_.accept_equals();
}

auto prepend_token::accept_char(char c) -> std::optional<location> {
  TENZIR_ASSERT(not token_);
  return next_.accept_char(c);
}

auto prepend_token::peek_char(char c) -> std::optional<location> {
  TENZIR_ASSERT(not token_);
  return next_.peek_char(c);
}

auto prepend_token::parse_operator() -> located<operator_ptr> {
  TENZIR_ASSERT(not token_);
  return next_.parse_operator();
}

auto prepend_token::parse_expression() -> tql::expression {
  TENZIR_ASSERT(not token_);
  return next_.parse_expression();
}

auto prepend_token::parse_legacy_expression() -> located<expression> {
  TENZIR_ASSERT(not token_);
  return next_.parse_legacy_expression();
}

auto prepend_token::parse_extractor() -> tql::extractor {
  TENZIR_ASSERT(not token_);
  return next_.parse_extractor();
}

auto prepend_token::parse_data() -> located<tenzir::data> {
  TENZIR_ASSERT(not token_);
  return next_.parse_data();
}

auto prepend_token::parse_int() -> located<int64_t> {
  TENZIR_ASSERT(not token_);
  return next_.parse_int();
}

auto prepend_token::at_end() -> bool {
  return not token_ && next_.at_end();
}

auto prepend_token::current_span() -> location {
  if (token_) {
    return token_->source;
  }
  return next_.current_span();
}

auto prepend_token::make_located_string() const -> located<std::string> {
  TENZIR_ASSERT(token_);
  return {std::string{token_->inner}, token_->source};
}

} // namespace tenzir
