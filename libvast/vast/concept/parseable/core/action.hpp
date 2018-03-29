/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/support/detail/action_traits.hpp"

namespace vast {

/// Executes a function after successfully parsing the inner attribute.
template <class Parser, class Action>
class action_parser : public parser<action_parser<Parser, Action>> {
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

  template <class Iterator, class Attribute, class A = Action>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if constexpr (detail::action_traits<A>::no_args_returns_void) {
      if (!parser_(f, l, a))
        return false;
      action_();
    } else if constexpr (detail::action_traits<A>::one_arg_returns_void) {
      if (!parser_(f, l, a))
        return false;
      action_(a);
    } else if constexpr (detail::action_traits<A>::no_args_returns_non_void) {
      inner_attribute x;
      if (!parser_(f, l, x))
        return false;
      a = action_();
    } else {
      // One argument, non-void return type.
      static_assert(detail::action_traits<A>::one_arg_returns_non_void);
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

