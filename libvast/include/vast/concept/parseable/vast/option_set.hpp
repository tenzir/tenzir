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

struct option_set_parser : parser_base<option_set_parser> {
  using attribute = std::unordered_map<std::string, data>;
  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& x) const {
    while (!parsers::eoi(f, l, unused)) {
      auto success = parse_short_form(f, l, x) && parse_long_form(f, l, x);
      if (!success) {
        return false;
      }
      if (!consume_space(f, l)) {
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
    constexpr auto space_consumer = ignore(+whitespace);
    return space_consumer(f, l, unused);
  }
  template <class Attribute>
  bool parse_short_form(auto& f, const auto& l, Attribute& x) const {
    static constexpr auto short_form_key_parser = '-' >> (parsers::alpha);
    auto short_form_opt = char{};
    auto f_previous = f;
    if (short_form_key_parser(f_previous, l, short_form_opt)) {
      vast::data data_out;
      auto short_form_data_parser = ignore(+whitespace) >> parsers::data;
      if (!short_form_data_parser(f_previous, l, data_out)) {
        return false;
      }
      auto found
        = std::find_if(defined_options_.begin(), defined_options_.end(),
                       [&short_form_opt](const auto& pair) {
                         return short_form_opt == pair.second;
                       });
      if (found != defined_options_.end()) {
        if constexpr (!std::is_same_v<Attribute, unused_type>) {
          x[std::move(found->first)] = std::move(data_out);
        }
        f = f_previous;
        return true;
      }
    }
    return true;
  }
  template <class Attribute>
  bool parse_long_form(auto& f, const auto& l, Attribute& x) const {
    static const auto long_form_key_parser
      = "--" >> +(parsers::alpha | parsers::chr{'-'});
    auto long_form_opt_key = std::string{};
    auto f_previous = f;
    if (long_form_key_parser(f_previous, l, long_form_opt_key)) {
      vast::data data_out;
      auto long_form_data_parser
        = ignore(*whitespace) >> '=' >> ignore(*whitespace) >> parsers::data;
      if (!long_form_data_parser(f_previous, l, data_out)) {
        return false;
      }
      auto found
        = std::find_if(defined_options_.begin(), defined_options_.end(),
                       [&long_form_opt_key](const auto& pair) {
                         return long_form_opt_key == pair.first;
                       });
      if (found != defined_options_.end()) {
        if constexpr (!std::is_same_v<Attribute, unused_type>) {
          x[std::move(found->first)] = std::move(data_out);
        }
        f = f_previous;
        return true;
      }
    }
    return true;
  }
  static constexpr auto whitespace = parsers::space;
  std::vector<std::pair<std::string, char>> defined_options_;
};

} // namespace vast
