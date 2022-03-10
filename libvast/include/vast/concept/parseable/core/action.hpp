//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/support/detail/action_traits.hpp"

namespace vast {

/// Executes a function after successfully parsing the inner attribute.
template <class Parser, class Action>
class action_parser : public parser_base<action_parser<Parser, Action>> {
public:
  using inner_attribute = typename Parser::attribute;
  using action_traits = detail::action_traits<Action>;
  using action_arg_type = typename action_traits::first_arg_type;

  using attribute =
    std::conditional_t<
      action_traits::returns_void,
      inner_attribute,
      typename action_traits::result_type
    >;

  action_parser(Parser p, Action fun) : parser_{std::move(p)}, action_(fun) {
    // nop
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if constexpr (detail::action_traits<Action>::no_args_returns_non_void) {
      if (!parser_(f, l, a))
        return false;
      a = action_();
    } else if constexpr (detail::action_traits<Action>::no_args_returns_void) {
      if (!parser_(f, l, a))
        return false;
      action_();
    } else if constexpr (detail::action_traits<Action>::one_arg_returns_void) {
      inner_attribute x;
      if (!parser_(f, l, x))
        return false;
      action_(std::move(x));
    } else {
      // One argument, non-void return type.
      static_assert(detail::action_traits<Action>::one_arg_returns_non_void);
      action_arg_type x;
      if (!parser_(f, l, x))
        return false;
      a = action_(std::move(x));
    }
    return true;
  }

private:
  Parser parser_;
  Action action_;
};

} // namespace vast
