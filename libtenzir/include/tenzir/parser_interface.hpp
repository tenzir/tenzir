//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/tql/expression.hpp"

#include <optional>
#include <string>
#include <utility>

namespace tenzir {

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

  virtual auto peek_char(char c) -> std::optional<location> = 0;

  virtual auto parse_operator() -> located<operator_ptr> = 0;

  virtual auto parse_expression() -> tql::expression = 0;

  virtual auto parse_legacy_expression() -> located<expression> = 0;

  virtual auto parse_extractor() -> tql::extractor = 0;

  virtual auto parse_data() -> located<data> = 0;

  virtual auto parse_int() -> located<int64_t> = 0;

  virtual auto at_end() -> bool = 0;

  virtual auto current_span() -> location = 0;
};

/// Wraps another `parser_interface`, but stops at a given keyword.
class until_keyword_parser final : public parser_interface {
public:
  explicit until_keyword_parser(std::string keyword, parser_interface& p)
    : keyword_{std::move(keyword)}, p_{p} {
  }

  auto parse_operator() -> located<operator_ptr> override;

  auto accept_identifier() -> std::optional<identifier> override;

  auto peek_identifier() -> std::optional<identifier> override;

  auto accept_equals() -> std::optional<location> override;

  auto accept_char(char c) -> std::optional<location> override;

  auto peek_char(char c) -> std::optional<location> override;

  auto parse_expression() -> tql::expression override;

  auto accept_shell_arg() -> std::optional<located<std::string>> override;

  auto peek_shell_arg() -> std::optional<located<std::string>> override;

  auto parse_legacy_expression() -> located<tenzir::expression> override;

  auto parse_extractor() -> tql::extractor override;

  auto parse_data() -> located<tenzir::data> override;

  auto parse_int() -> located<int64_t> override;

  auto at_end() -> bool override;

  auto current_span() -> location override;

private:
  std::string keyword_;
  parser_interface& p_;
};

} // namespace tenzir
