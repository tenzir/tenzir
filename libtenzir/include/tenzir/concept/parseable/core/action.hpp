//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/support/detail/action_traits.hpp"

#include <tuple>
#include <utility>

namespace tenzir {

/// Executes a function after successfully parsing the inner attribute.
template <class Parser, class Action>
class action_parser : public parser_base<action_parser<Parser, Action>> {
public:
  using inner_attribute = typename Parser::attribute;
  using action_traits = detail::action_traits<Action>;
  using action_arg_type = typename action_traits::first_arg_type;

  using attribute
    = std::conditional_t<action_traits::returns_void, inner_attribute,
                         typename action_traits::result_type>;

  action_parser(Parser p, Action fun) : parser_{std::move(p)}, action_(fun) {
    // nop
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    if constexpr (action_traits::arity == 0) {
      if (not parser_(f, l, a)) {
        return false;
      }
      if constexpr (action_traits::returns_void) {
        action_();
      } else {
        a = action_();
      }
    } else if constexpr (action_traits::arity == 1
                         and action_traits::returns_void) {
      inner_attribute x;
      if (not parser_(f, l, x)) {
        return false;
      }
      action_(std::move(x));
    } else if constexpr (action_traits::arity == 1) {
      action_arg_type x;
      if (not parser_(f, l, x)) {
        return false;
      }
      a = action_(std::move(x));
    } else {
      inner_attribute x;
      if (not parser_(f, l, x)) {
        return false;
      }
      if constexpr (action_traits::returns_void) {
        std::apply(action_, std::move(x));
      } else {
        a = std::apply(action_, std::move(x));
      }
    }
    return true;
  }

private:
  Parser parser_;
  Action action_;
};

} // namespace tenzir
