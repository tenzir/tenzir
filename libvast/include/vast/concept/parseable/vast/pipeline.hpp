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

#include <fmt/format.h>

namespace vast::parsers {
using namespace parser_literals;
constexpr inline auto required_ws = ignore(+space);
constexpr inline auto optional_ws = ignore(*space);
constexpr inline auto end_of_pipeline_operator = ('|' | eoi);
constexpr inline auto extractor_char = alnum | chr{'_'} | chr{'-'} | chr{':'};
// An extractor cannot start with:
//  - '-' to leave room for potential arithmetic expressions in operands
const inline auto extractor
  = (!('-'_p) >> (+extractor_char % '.')).then([](std::vector<std::string> in) {
      return fmt::to_string(fmt::join(in.begin(), in.end(), "."));
    });
const inline auto extractor_list
  = (extractor % (optional_ws >> ',' >> optional_ws));
const inline auto extractor_assignment
  = (extractor >> optional_ws >> '=' >> optional_ws >> extractor);
const inline auto extractor_assignment_list
  = (extractor_assignment % (',' >> optional_ws));
constexpr inline auto aggregation_func_char = alnum | chr{'-'};
const inline auto aggregation_function
  = -(extractor >> optional_ws >> '=' >> optional_ws)
    >> (+aggregation_func_char) >> optional_ws >> '(' >> optional_ws
    >> extractor_list >> optional_ws >> ')';
const inline auto aggregation_function_list
  = (aggregation_function % (',' >> optional_ws));
} // namespace vast::parsers
