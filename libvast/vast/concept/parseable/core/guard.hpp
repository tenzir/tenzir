//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast {

/// Attaches a guard expression to a parser that must succeed after the parser
/// executes.
/// @tparam Parser The parser to augment with a guard expression.
/// @tparam Guard A function that takes the synthesized attribute by
///               const-reference and returns `bool`.
template <class Parser, class Guard>
class guard_parser : public parser_base<guard_parser<Parser, Guard>> {
public:
  using inner_attribute = typename Parser::attribute;
  using return_type = std::invoke_result_t<Guard, inner_attribute>;
  static constexpr bool returns_bool = std::is_same_v<bool, return_type>;
  using attribute = std::conditional_t<returns_bool, inner_attribute,
                                       detail::remove_optional_t<return_type>>;

  guard_parser(Parser p, Guard fun) : parser_{std::move(p)}, guard_(fun) {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    inner_attribute attr;
    if constexpr (returns_bool) {
      if (!(parser_(f, l, attr) && guard_(attr)))
        return false;
      a = Attribute(std::move(attr));
      return true;
    } else {
      if (!(parser_(f, l, attr)))
        return false;
      auto fin = guard_(std::move(attr));
      if (!fin)
        return false;
      a = Attribute(*std::move(fin));
      return true;
    }
  }

private:
  Parser parser_;
  Guard guard_;
};

} // namespace vast
