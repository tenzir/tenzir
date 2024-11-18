//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/choice.hpp"
#include "tenzir/concept/parseable/core/operators.hpp"
#include "tenzir/concept/parseable/core/plus.hpp"
#include "tenzir/concept/parseable/string/char.hpp"
#include "tenzir/concept/parseable/string/char_class.hpp"
#include "tenzir/concept/parseable/string/string.hpp"
#include "tenzir/concept/parseable/tenzir/data.hpp"
#include "tenzir/concept/parseable/tenzir/identifier.hpp"
#include "tenzir/detail/string.hpp"

#include <fmt/format.h>

namespace tenzir::parsers {

const inline auto comment_start = str{"/*"};
/// Parses a '/* ... */' style comment. The attribute of the parser is the
/// comment between the '/*' and '*/' delimiters.
const inline auto comment
  = comment_start >> *(any - (chr{'*'} >> &chr{'/'})) >> "*/";

const inline auto required_ws_or_comment = ignore(+(space | comment));
const inline auto optional_ws_or_comment = ignore(*(space | comment));

constexpr inline auto end_of_pipeline_operator = ('|' | eoi);
constexpr inline auto extractor_char = alnum | chr{'_'} | chr{'-'} | chr{':'};
// An extractor cannot start with:
//  - '-' to leave room for potential arithmetic expressions in operands
const inline auto extractor
  = (!(chr{'-'}) >> (+extractor_char % '.'))
      .then([](std::vector<std::string> in) {
        return fmt::to_string(fmt::join(in.begin(), in.end(), "."));
      });
const inline auto extractor_list
  = (extractor % (optional_ws_or_comment >> ',' >> optional_ws_or_comment));
const inline auto extractor_assignment
  = (extractor >> optional_ws_or_comment >> '=' >> optional_ws_or_comment
     >> extractor);
const inline auto extractor_assignment_list
  = (extractor_assignment
     % (optional_ws_or_comment >> ',' >> optional_ws_or_comment));
const inline auto extractor_value_assignment
  = (extractor >> optional_ws_or_comment >> '=' >> optional_ws_or_comment
     >> data);
const inline auto extractor_value_assignment_list
  = (extractor_value_assignment
     % (optional_ws_or_comment >> ',' >> optional_ws_or_comment));
const inline auto aggregation_function
  = -(extractor >> optional_ws_or_comment >> '=' >> optional_ws_or_comment)
    >> plugin_name >> optional_ws_or_comment >> '(' >> optional_ws_or_comment
    >> (extractor | str{"."}) >> optional_ws_or_comment >> ')';
const inline auto aggregation_function_list
  = (aggregation_function % (',' >> optional_ws_or_comment));

const inline auto unquoted_operator_arg
  = &!(chr{'\''} | '"') >> +(printable - '|' - space - comment_start);

const inline auto operator_arg = qstr | qqstr | unquoted_operator_arg;

namespace detail {

constexpr inline auto or_default
  = [](std::optional<std::vector<std::string>> x) -> std::vector<std::string> {
  // Note: `std::optional::value_or` always performs a copy.
  if (!x) {
    return {};
  }
  return std::move(*x);
};

} // namespace detail

// Multiple operator arguments are separated by whitespace or comments.
const inline auto operator_args
  = (-(operator_arg % required_ws_or_comment)).then(detail::or_default);

/// Parses `arg*`, but stops if the keyword is encountered.
inline auto operator_args_before(const std::string& keyword) {
  return (-((operator_arg - keyword) % required_ws_or_comment))
    .then(detail::or_default);
}

/// Parses `name arg*`.
const inline auto name_args
  = optional_ws_or_comment >> plugin_name >> optional_ws_or_comment
    >> operator_args >> optional_ws_or_comment >> end_of_pipeline_operator;

/// Parses `name arg* (KEYWORD name arg*)?`.
inline auto name_args_opt_keyword_name_args(const std::string& keyword) {
  // clang-format off
  return optional_ws_or_comment
    >> plugin_name
    >> (-(required_ws_or_comment
      >> ((operator_arg - keyword) % required_ws_or_comment))
    ).then(detail::or_default)
    >> -(required_ws_or_comment
      >> keyword
      >> required_ws_or_comment
      >> plugin_name
      >> (-(required_ws_or_comment
        >> (operator_arg % required_ws_or_comment))
      ).then(detail::or_default)
    )
    >> optional_ws_or_comment
    >> end_of_pipeline_operator;
  // clang-format on
}

} // namespace tenzir::parsers

namespace tenzir {

/// Escapes a string such that it can be safely used as an operator argument.
/// It generally tries tries to avoid quotes, but it will also quote the words
/// `from`, `read`, `write` and `to`.
///
/// Guarantees `operator_arg.apply(operator_arg_escape(y)).value() == y` for
/// every `y == operator_arg.apply(x).value()`.
inline auto escape_operator_arg(std::string_view x) -> std::string {
  auto f = x.begin();
  if ((parsers::unquoted_operator_arg >> parsers::eoi)(f, x.end(), unused)) {
    for (auto y : {"from", "read", "write", "to"}) {
      if (x == y) {
        return fmt::format("'{}'", x);
      }
    }
    return std::string{x};
  }
  return '\'' + detail::replace_all(std::string{x}, "'", "\\'") + '\'';
}

/// The multi-argument version of @see operator_arg_escape.
template <class Range>
inline auto escape_operator_args(Range&& r) -> std::string {
  auto result = std::string{};
  auto first = true;
  for (auto&& x : r) {
    if (!first) {
      result += ' ';
    }
    first = false;
    result += escape_operator_arg(x);
  }
  return result;
}

} // namespace tenzir
