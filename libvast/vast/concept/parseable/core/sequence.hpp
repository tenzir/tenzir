//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/support/detail/attr_fold.hpp"
#include "vast/concept/support/detail/sequence.hpp"
#include "vast/detail/type_traits.hpp"

#include <tuple>
#include <type_traits>

namespace vast {

template <class Lhs, class Rhs>
class sequence_parser;

template <class>
struct is_sequence_parser : std::false_type {};

template <class... Ts>
struct is_sequence_parser<sequence_parser<Ts...>> : std::true_type {};

template <class T>
constexpr bool is_sequence_parser_v = is_sequence_parser<T>::value;

template <class Lhs, class Rhs>
class sequence_parser : public parser<sequence_parser<Lhs, Rhs>> {
public:
  using lhs_type = Lhs;
  using rhs_type = Rhs;
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;

  // LHS = unused && RHS = unused  =>  unused
  // LHS = T && RHS = unused       =>  LHS
  // LHS = unused && RHS = T       =>  RHS
  // LHS = T && RHS = U            =>  std:tuple<T, U>
  using attribute =
    std::conditional_t<
      std::is_same_v<lhs_attribute, unused_type>
        && std::is_same_v<rhs_attribute, unused_type>,
      unused_type,
      std::conditional_t<
        std::is_same_v<lhs_attribute, unused_type>,
        rhs_attribute,
        std::conditional_t<
          std::is_same_v<rhs_attribute, unused_type>,
          lhs_attribute,
          detail::attr_fold_t<
            decltype(std::tuple_cat(detail::tuple_wrap<lhs_attribute>{},
                                    detail::tuple_wrap<rhs_attribute>{}))
          >
        >
      >
    >;

  constexpr sequence_parser(Lhs lhs, Rhs rhs)
    : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    auto save = f;
    if (parse_left(f, l, a) && parse_right(f, l, a))
      return true;
    f = save;
    return false;
  }

private:
  template <class Iterator, class... Ts>
  bool parse_left(Iterator& f, const Iterator& l, std::tuple<Ts...>& x) const {
    return lhs_(f, l, detail::access_left<sequence_parser>(x));
  }

  template <class Iterator, class... Ts>
  bool parse_right(Iterator& f, const Iterator& l, std::tuple<Ts...>& x) const {
    return rhs_(f, l, detail::access_right<sequence_parser>(x));
  }

  template <class Iterator>
  bool parse_left(Iterator& f, const Iterator& l, unused_type) const {
    return lhs_(f, l, unused);
  }

  template <class Iterator>
  bool parse_right(Iterator& f, const Iterator& l, unused_type) const {
    return rhs_(f, l, unused);
  }

  template <class Iterator, class T, class U>
  bool parse_left(Iterator& f, const Iterator& l, std::pair<T, U>& p) const {
    return lhs_(f, l, p.first);
  }

  template <class Iterator, class T, class U>
  bool parse_right(Iterator& f, const Iterator& l, std::pair<T, U>& p) const {
    return rhs_(f, l, p.second);
  }

  template <class Iterator, class Attribute>
  bool parse_left(Iterator& f, const Iterator& l, Attribute& a) const {
    return lhs_(f, l, a);
  }

  template <class Iterator, class Attribute>
  bool parse_right(Iterator& f, const Iterator& l, Attribute& a) const {
    return rhs_(f, l, a);
  }

  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast
