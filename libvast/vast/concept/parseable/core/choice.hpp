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

#include <type_traits>

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/support/detail/variant.hpp"

#include "vast/detail/type_traits.hpp"

namespace vast {

template <class Lhs, class Rhs>
class choice_parser;

template <class>
struct is_choice_parser : std::false_type {};

template <class Lhs, class Rhs>
struct is_choice_parser<choice_parser<Lhs, Rhs>> : std::true_type {};

template <class T>
constexpr bool is_choice_parser_v = is_choice_parser<T>::value;

/// Attempts to parse either LHS or RHS.
template <class Lhs, class Rhs>
class choice_parser : public parser<choice_parser<Lhs, Rhs>> {
public:
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;

  // LHS = unused && RHS = unused  =>  unused
  // LHS = T && RHS = unused       =>  LHS
  // LHS = unused && RHS = T       =>  RHS
  // LHS = T && RHS = T            =>  T
  // LHS = T && RHS = U            =>  variant<T, U>
  using attribute =
    std::conditional_t<
      std::is_same<lhs_attribute, unused_type>{}
        && std::is_same<rhs_attribute, unused_type>{},
      unused_type,
      std::conditional_t<
        std::is_same<lhs_attribute, unused_type>{},
        rhs_attribute,
        std::conditional_t<
          std::is_same<rhs_attribute, unused_type>{},
          lhs_attribute,
          std::conditional_t<
            std::is_same<lhs_attribute, rhs_attribute>{},
            lhs_attribute,
            detail::flattened_variant<lhs_attribute, rhs_attribute>
          >
        >
      >
    >;

  choice_parser(Lhs lhs, Rhs rhs)
    : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    auto save = f;
    if (parse_left<Lhs>(f, l, a))
      return true;
    f = save;
    if (parse_right(f, l, a))
      return true;
    f = save;
    return false;
  }

private:
  template <class Left, class Iterator, class Attribute>
  auto parse_left(Iterator& f, const Iterator& l, Attribute& a) const
  -> std::enable_if_t<is_choice_parser<Left>{}, bool> {
    return lhs_(f, l, a); // recurse
  }

  template <class Left, class Iterator>
  auto parse_left(Iterator& f, const Iterator& l, unused_type) const
  -> std::enable_if_t<!is_choice_parser_v<Left>, bool> {
    return lhs_(f, l, unused);
  }

  template <class Left, class Iterator, class Attribute>
  auto parse_left(Iterator& f, const Iterator& l, Attribute& a) const
  -> std::enable_if_t<!is_choice_parser_v<Left>, bool> {
    lhs_attribute al;
    if (!lhs_(f, l, al))
      return false;
    a = std::move(al);
    return true;
  }

  template <class Iterator, class Attribute>
  bool parse_right(Iterator& f, const Iterator& l, Attribute& a) const {
    if constexpr (detail::is_any_v<unused_type, Attribute, rhs_attribute>) {
      return rhs_(f, l, unused);
    } else {
      rhs_attribute ar;
      if (!rhs_(f, l, ar))
        return false;
      a = std::move(ar);
      return true;
    }
  }

  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast

