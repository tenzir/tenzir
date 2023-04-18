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

// Multiple operator arguments are separated by whitespace or comments.
const inline auto operator_args
  = (-(operator_arg % required_ws_or_comment))
      .then([](caf::optional<std::vector<std::string>> x) {
        VAST_INFO("-* {}", x);
        return std::move(x).value_or({});
      });

/// Parses `ARG*`, but stops if the keyword is encountered.
inline auto operator_args_before(const std::string& keyword) {
  return (-((operator_arg - keyword) % required_ws_or_comment))
    .then([](caf::optional<std::vector<std::string>> x) {
      VAST_INFO("-> {}", x);
      return std::move(x).value_or({});
    });
}

/// Parses `NAME ARG* (KEYWORD NAME ARG*)?`.
inline auto name_args_opt_keyword_name_args(const std::string& keyword) {
  return optional_ws_or_comment >> plugin_name >> optional_ws_or_comment
         >> operator_args_before(keyword) >> optional_ws_or_comment
         >> -(keyword >> plugin_name >> optional_ws_or_comment >> operator_args
              >> optional_ws_or_comment)
         >> end_of_pipeline_operator;
}

// " stdin read json"
//         ^

// clang-format off
using result_type = std::tuple<
  std::string,                // plugin name (stdin)
  std::vector<std::string>,   // plugin args ([])
  caf::optional<std::tuple<   // rhs
    std::string,              // plugin name (json)
    std::vector<std::string>  // plugin args ([])
  >>
>;
// clang-format on

template <class Iterator>
inline auto
parse_name_args_opt_keyword_name_args(const std::string& keyword, Iterator& f,
                                      const Iterator& l)
  -> std::optional<result_type> {
  auto parse_result = result_type{};
  // clang-format off
  const auto p =
       optional_ws_or_comment
    // Parse name
    >> plugin_name
    >> (-(required_ws_or_comment >> ((operator_arg - keyword) % required_ws_or_comment)))
    >> -(required_ws_or_comment >> keyword
          >> required_ws_or_comment
          >> plugin_name
          >> (-(required_ws_or_comment >> (operator_arg % required_ws_or_comment))).then([](caf::optional<std::vector<std::string>> x) {
            return x.value_or({});
          })
        )
    >> optional_ws_or_comment
    >> end_of_pipeline_operator;
  // clang-format on

  if (!p(f, l, parse_result)) {
    return std::nullopt;
  }
  return parse_result;

  // if (not name_args_opt_keyword_name_args(keyword)(f, l, parsed)) {
  //   return std::nullopt;
  // }
  // die("todo");
  // auto&& [first, first_args, rest] = parsed;
  // auto&& [second, second_args] = rest;
  // if (second_args) {
  //   return std::tuple{std::move(first), std::move(first_args),
  //                     std::pair{std::move(second), std::move(*second_args)}};
  // } else {
  //   return std::tuple{std::move(first), std::move(first_args), std::nullopt};
  // }
}

} // namespace vast::parsers
