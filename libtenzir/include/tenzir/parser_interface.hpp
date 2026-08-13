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
#include "tenzir/option.hpp"
#include "tenzir/tql/expression.hpp"

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

  virtual auto accept_shell_arg() -> Option<located<std::string>> = 0;

  virtual auto peek_shell_arg() -> Option<located<std::string>> = 0;

  virtual auto accept_identifier() -> Option<identifier> = 0;

  virtual auto peek_identifier() -> Option<identifier> = 0;

  virtual auto accept_equals() -> Option<location> = 0;

  virtual auto accept_char(char c) -> Option<location> = 0;

  virtual auto peek_char(char c) -> Option<location> = 0;

  virtual auto parse_expression() -> tql::expression = 0;

  virtual auto parse_legacy_expression() -> located<expression> = 0;

  virtual auto parse_extractor() -> tql::extractor = 0;

  virtual auto parse_data() -> located<data> = 0;

  virtual auto parse_int() -> located<int64_t> = 0;

  virtual auto at_end() -> bool = 0;

  virtual auto current_span() -> location = 0;
};

} // namespace tenzir
