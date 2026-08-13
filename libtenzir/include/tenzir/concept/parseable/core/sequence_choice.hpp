//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/optional.hpp"
#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/option.hpp"

#include <tuple>
#include <type_traits>

namespace tenzir {

// (LHS >> ~RHS) | RHS
template <class Lhs, class Rhs>
class sequence_choice_parser
  : public parser_base<sequence_choice_parser<Lhs, Rhs>> {
public:
  using lhs_type = Lhs;
  using rhs_type = Rhs;
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;

  // LHS = unused && RHS = unused  =>  unused
  // LHS = T && RHS = unused       =>  Option<LHS>
  // LHS = unused && RHS = T       =>  Option<RHS>
  // LHS = T && RHS = U            =>  std:tuple<Option<LHS>,
  // Option<RHS>>
  using attribute = std::conditional_t<
    std::is_same<lhs_attribute, unused_type>{}
      and std::is_same<rhs_attribute, unused_type>{},
    unused_type,
    std::conditional_t<
      std::is_same<rhs_attribute, unused_type>{}, Option<lhs_attribute>,
      std::conditional_t<
        std::is_same<lhs_attribute, unused_type>{}, Option<rhs_attribute>,
        std::tuple<Option<lhs_attribute>, Option<rhs_attribute>>>>>;

  sequence_choice_parser(Lhs lhs, Rhs rhs)
    : lhs_{std::move(lhs)}, rhs_{rhs}, rhs_opt_{std::move(rhs)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    Option<rhs_attribute> rhs_attr;
    if (lhs_(f, l, left_attr(a)) and rhs_opt_(f, l, rhs_attr)) {
      right_attr(a) = std::move(rhs_attr);
      return true;
    }
    return rhs_(f, l, right_attr(a));
  }

private:
  template <class Attribute, class L = lhs_attribute, class R = rhs_attribute>
    requires(std::is_same_v<L, unused_type>)
  static auto left_attr(Attribute&) -> unused_type& {
    return unused;
  }

  template <class Attribute, class L = lhs_attribute, class R = rhs_attribute>
  static auto left_attr(Attribute& a) -> Option<L>&
    requires(not std::is_same_v<L, unused_type>
             and std::is_same_v<R, unused_type>)
  {
    return a;
  }

  template <class... Ts, class L = lhs_attribute, class R = rhs_attribute>
  static auto left_attr(std::tuple<Ts...>& t) -> Option<L>&
    requires(not(std::is_same_v<L, unused_type>
                 or std::is_same_v<R, unused_type>))
  {
    return std::get<0>(t);
  }

  template <class Attribute, class L = lhs_attribute, class R = rhs_attribute>
    requires(std::is_same_v<R, unused_type>)
  static auto right_attr(Attribute&) -> unused_type& {
    return unused;
  }

  template <class Attribute, class L = lhs_attribute, class R = rhs_attribute>
  static auto right_attr(Attribute& a) -> Option<R>&
    requires(std::is_same_v<L, unused_type>
             and not std::is_same_v<R, unused_type>)
  {
    return a;
  }

  template <class... Ts, class L = lhs_attribute, class R = rhs_attribute>
  static auto right_attr(std::tuple<Ts...>& t) -> Option<R>&
    requires(not(std::is_same_v<L, unused_type>
                 or std::is_same_v<R, unused_type>))
  {
    return std::get<1>(t);
  }

  lhs_type lhs_;
  rhs_type rhs_;
  optional_parser<rhs_type> rhs_opt_;
};

} // namespace tenzir
