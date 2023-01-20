//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/char_class.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/data.hpp"

#include <algorithm>

namespace vast {
using namespace parser_literals;

struct option_set_parser : parser_base<option_set_parser> {
  using attribute = std::unordered_map<std::string, data>;

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    auto short_form_option_parser
      = '-' >> (parsers::alpha) >> ignore(+parsers::space) >> parsers::data;
    auto long_form_option_parser = "--" >> +(parsers::alpha)
                                   >> ignore(*parsers::space) >> '='
                                   >> ignore(*parsers::space) >> parsers::data;
    auto first = true;
    while (true) {
      if (parsers::eoi(f, l, unused)) {
        break;
      }
      if (!first) {
        auto space_consumer = ignore(+parsers::space);
        if (!space_consumer(f, l, unused)) {
          break;
        }
      }
      first = false;
      auto previous_f = f;
      auto short_form_option = std::tuple<char, vast::data>{};
      if (short_form_option_parser(f, l, short_form_option)) {
        auto found
          = std::find_if(defined_options_.begin(), defined_options_.end(),
                         [&short_form_option](const auto& pair) {
                           return std::get<0>(short_form_option) == pair.second;
                         });
        if (found != defined_options_.end()) {
          if constexpr (!std::is_same_v<Attribute, unused_type>) {
            x[found->first] = std::get<1>(short_form_option);
          }
          continue;
        }
      };
      f = previous_f;
      auto long_form_option = std::tuple<std::string, vast::data>{};
      if (long_form_option_parser(f, l, long_form_option)) {
        auto found
          = std::find_if(defined_options_.begin(), defined_options_.end(),
                         [&long_form_option](const auto& pair) {
                           return std::get<0>(long_form_option) == pair.first;
                         });
        if (found != defined_options_.end()) {
          if constexpr (!std::is_same_v<Attribute, unused_type>) {
            x[found->first] = std::get<1>(long_form_option);
          }
          continue;
        }
      };
      f = previous_f;
      break;
    }
    return true;
  }

  explicit option_set_parser(
    std::vector<std::pair<std::string, char>> defined_options)
    : defined_options_{std::move(defined_options)} {
  }

private:
  std::vector<std::pair<std::string, char>> defined_options_;
};

} // namespace vast
