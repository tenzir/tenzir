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

} // namespace vast::parsers
