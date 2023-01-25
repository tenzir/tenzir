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
    while (!parsers::eoi(f, l, unused)) {
      auto success = parse_short_form(f, l, x) || parse_long_form(f, l, x);
      if (!success || !consume_space(f, l)) {
        break;
      }
    }
    return true;
  }
  explicit option_set_parser(
    std::vector<std::pair<std::string, char>> defined_options)
    : defined_options_{std::move(defined_options)} {
  }

private:
  static bool consume_space(auto& f, const auto& l) {
    constexpr auto space_consumer = ignore(+parsers::space);
    return space_consumer(f, l, unused);
  }
  template <class Attribute>
  bool parse_short_form(auto& f, const auto& l, Attribute& x) const {
    static constexpr auto short_form_parser
      = '-' >> (parsers::alpha) >> ignore(+parsers::space) >> parsers::data;
    auto short_form_opt = std::tuple<char, vast::data>{};
    auto f_previous = f;
    if (short_form_parser(f_previous, l, short_form_opt)) {
      auto found
        = std::find_if(defined_options_.begin(), defined_options_.end(),
                       [&short_form_opt](const auto& pair) {
                         return std::get<0>(short_form_opt) == pair.second;
                       });
      if (found != defined_options_.end()) {
        if constexpr (!std::is_same_v<Attribute, unused_type>) {
          x[std::move(found->first)] = std::move(std::get<1>(short_form_opt));
        }
        f = f_previous;
        return true;
      }
    }
    return false;
  }
  template <class Attribute>
  bool parse_long_form(auto& f, const auto& l, Attribute& x) const {
    static const auto parser = "--" >> +(parsers::alpha)
                               >> ignore(*parsers::space) >> '='
                               >> ignore(*parsers::space) >> parsers::data;
    auto long_form_opt = std::tuple<std::string, vast::data>{};
    auto f_copy = f;
    if (parser(f_copy, l, long_form_opt)) {
      auto found
        = std::find_if(defined_options_.begin(), defined_options_.end(),
                       [&long_form_opt](const auto& pair) {
                         return std::get<0>(long_form_opt) == pair.first;
                       });
      if (found != defined_options_.end()) {
        if constexpr (!std::is_same_v<Attribute, unused_type>) {
          x[std::move(found->first)] = std::move(std::get<1>(long_form_opt));
        }
        f = f_copy;
        return true;
      }
    }
    return false;
  }

private:
  std::vector<std::pair<std::string, char>> defined_options_;
};

} // namespace vast
