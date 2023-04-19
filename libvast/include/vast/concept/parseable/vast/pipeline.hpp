//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/choice.hpp"
#include "vast/concept/parseable/core/operators.hpp"
#include "vast/concept/parseable/core/plus.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/vast/data.hpp"

#include <fmt/format.h>

namespace vast::parsers {

/// Parses a '/* ... */' style comment. The attribute of the parser is the
/// comment between the '/*' and '*/' delimiters.
const inline auto comment = "/*" >> *(any - (chr{'*'} >> &chr{'/'})) >> "*/";

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
constexpr inline auto aggregation_func_char = alnum | chr{'-'};
const inline auto aggregation_function
  = -(extractor >> optional_ws_or_comment >> '=' >> optional_ws_or_comment)
    >> (+aggregation_func_char) >> optional_ws_or_comment >> '('
    >> optional_ws_or_comment >> extractor_list >> optional_ws_or_comment
    >> ')';
const inline auto aggregation_function_list
  = (aggregation_function % (',' >> optional_ws_or_comment));

// An operator argument can be ...
const inline auto operator_arg =
  // ... a single quoted string
  qstr
  // ... or a double quoted string
  | qqstr
  // ... or something that does not start with a quote
  | &!(chr{'\''} | '"')
      // ... and contains no whitespace, comments or pipes.
      >> +(printable - '|' - required_ws_or_comment);

namespace detail {

constexpr inline auto or_default
  = [](caf::optional<std::vector<std::string>> x) -> std::vector<std::string> {
  // Note: `caf::optional::value_or` always performs a copy.
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

} // namespace vast::parsers
