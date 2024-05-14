//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/parser_interface.hpp"

namespace tenzir {

class prepend_token final : public parser_interface {
public:
  prepend_token(located<std::string_view> token, parser_interface& next)
    : token_{std::move(token)}, next_{next} {
  }
  prepend_token(std::nullopt_t, parser_interface& next)
    : token_{std::nullopt}, next_{next} {
  }

  auto accept_shell_arg() -> std::optional<located<std::string>> override {
    if (token_) {
      auto tmp = make_located_string();
      token_ = std::nullopt;
      return tmp;
    }
    return next_.accept_shell_arg();
  }

  auto peek_shell_arg() -> std::optional<located<std::string>> override {
    if (token_) {
      return make_located_string();
    }
    return next_.peek_shell_arg();
  }

  auto accept_identifier() -> std::optional<identifier> override {
    TENZIR_ASSERT(not token_);
    return next_.accept_identifier();
  }

  auto peek_identifier() -> std::optional<identifier> override {
    TENZIR_ASSERT(not token_);
    return next_.peek_identifier();
  }

  auto accept_equals() -> std::optional<location> override {
    TENZIR_ASSERT(not token_);
    return next_.accept_equals();
  }

  auto accept_char(char c) -> std::optional<location> override {
    TENZIR_ASSERT(not token_);
    return next_.accept_char(c);
  }

  auto peek_char(char c) -> std::optional<location> override {
    TENZIR_ASSERT(not token_);
    return next_.peek_char(c);
  }

  auto parse_operator() -> located<operator_ptr> override {
    TENZIR_ASSERT(not token_);
    return next_.parse_operator();
  }

  auto parse_expression() -> tql::expression override {
    TENZIR_ASSERT(not token_);
    return next_.parse_expression();
  }

  auto parse_legacy_expression() -> located<expression> override {
    TENZIR_ASSERT(not token_);
    return next_.parse_legacy_expression();
  }

  auto parse_extractor() -> tql::extractor override {
    TENZIR_ASSERT(not token_);
    return next_.parse_extractor();
  }

  auto parse_data() -> located<tenzir::data> override {
    TENZIR_ASSERT(not token_);
    return next_.parse_data();
  }

  auto parse_int() -> located<int64_t> override {
    TENZIR_ASSERT(not token_);
    return next_.parse_int();
  }

  auto at_end() -> bool override {
    return not token_ && next_.at_end();
  }

  auto current_span() -> location override {
    if (token_) {
      return token_->source;
    }
    return next_.current_span();
  }

private:
  auto make_located_string() const -> located<std::string> {
    TENZIR_ASSERT(token_);
    return {std::string{token_->inner}, token_->source};
  }

  std::optional<located<std::string_view>> token_;
  parser_interface& next_;
};

} // namespace tenzir
