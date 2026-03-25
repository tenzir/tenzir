//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/parser_interface.hpp"

#include <string_view>
#include <utility>

namespace tenzir {

class prepend_token final : public parser_interface {
public:
  prepend_token(located<std::string_view> token, parser_interface& next)
    : token_{std::move(token)}, next_{next} {
  }
  prepend_token(std::nullopt_t, parser_interface& next)
    : token_{std::nullopt}, next_{next} {
  }

  auto accept_shell_arg() -> std::optional<located<std::string>> override;

  auto peek_shell_arg() -> std::optional<located<std::string>> override;

  auto accept_identifier() -> std::optional<identifier> override;

  auto peek_identifier() -> std::optional<identifier> override;

  auto accept_equals() -> std::optional<location> override;

  auto accept_char(char c) -> std::optional<location> override;

  auto peek_char(char c) -> std::optional<location> override;

  auto parse_operator() -> located<operator_ptr> override;

  auto parse_expression() -> tql::expression override;

  auto parse_legacy_expression() -> located<expression> override;

  auto parse_extractor() -> tql::extractor override;

  auto parse_data() -> located<tenzir::data> override;

  auto parse_int() -> located<int64_t> override;

  auto at_end() -> bool override;

  auto current_span() -> location override;

private:
  auto make_located_string() const -> located<std::string>;

  std::optional<located<std::string_view>> token_;
  parser_interface& next_;
};

} // namespace tenzir
