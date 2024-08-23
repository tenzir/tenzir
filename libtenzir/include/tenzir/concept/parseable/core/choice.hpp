//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/parseable/core/parser.hpp"
#include "tenzir/concept/support/detail/variant.hpp"
#include "tenzir/detail/type_traits.hpp"

#include <type_traits>

namespace tenzir {

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
class choice_parser : public parser_base<choice_parser<Lhs, Rhs>> {
private:
  using lhs_attribute = typename Lhs::attribute;
  using rhs_attribute = typename Rhs::attribute;

  // LHS = unused && RHS = unused  =>  unused
  // LHS = unused && RHS = T       =>  RHS
  // LHS = T && RHS = unused       =>  LHS
  // LHS = T && RHS = T            =>  T
  // LHS = T && RHS = U            =>  variant<T, U>
  static constexpr auto attribute_type() {
    if constexpr (std::is_same_v<
                    lhs_attribute,
                    unused_type> && std::is_same_v<rhs_attribute, unused_type>)
      return unused_type{};
    else if constexpr (std::is_same_v<lhs_attribute, unused_type>)
      return rhs_attribute{};
    else if constexpr (
      std::is_same_v<rhs_attribute,
                     unused_type> || std::is_same_v<lhs_attribute, rhs_attribute>)
      return lhs_attribute{};
    else
      return detail::flattened_variant<lhs_attribute, rhs_attribute>{};
  }

public:
  using attribute = decltype(attribute_type());

  constexpr choice_parser(Lhs lhs, Rhs rhs)
    : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  template <class Iterator, class Attribute>
  bool parse(Iterator& f, const Iterator& l, Attribute& a) const {
    auto save = f;
    if (do_parse(lhs_, f, l, a))
      return true;
    f = save;
    if (do_parse(rhs_, f, l, a))
      return true;
    f = save;
    return false;
  }

private:
  template <class Parser, class Iterator, class Attribute>
  bool do_parse(const Parser& p, Iterator& f, const Iterator& l,
                Attribute& a) const {
    using detail::is_any_v;
    using parser_attribute = typename Parser::attribute;
    if constexpr (is_choice_parser_v<Parser>) {
      // If LHS/RHS is a choice parser, we can recurse because the passed-in
      // attribute will also be valid for the sub parser.
      return p(f, l, a);
    } else if constexpr (is_any_v<unused_type, Attribute, parser_attribute>) {
      // If LHS/RHS has an unused attribute or the passed-in attribute is
      // unused, then we won't have to parse into a concrete object.
      return p(f, l, unused);
    } else {
      // Parse one element of the variant and assign it to the passed-in
      // attribute.
      auto attr = parser_attribute{};
      if (!p(f, l, attr))
        return false;
      a = std::move(attr);
      return true;
    }
  }

  Lhs lhs_;
  Rhs rhs_;
};

} // namespace tenzir
