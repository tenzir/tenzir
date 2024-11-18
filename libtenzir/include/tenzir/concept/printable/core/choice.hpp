//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/support/detail/variant.hpp"
#include "tenzir/variant_traits.hpp"

#include <type_traits>

namespace tenzir {

template <class Lhs, class Rhs>
class choice_printer_t;

template <class>
struct is_choice_printer : std::false_type {};

template <class Lhs, class Rhs>
struct is_choice_printer<choice_printer_t<Lhs, Rhs>> : std::true_type {};

template <class T>
concept choice_printer = is_choice_printer<T>::value;

/// Attempts to print either LHS or RHS.
template <class Lhs, class Rhs>
class choice_printer_t : public printer_base<choice_printer_t<Lhs, Rhs>> {
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

  constexpr choice_printer_t(Lhs lhs, Rhs rhs)
    : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute& a) const {
    return print_left<Lhs>(out, a) || print_right(out, a);
  }

private:
  template <choice_printer Left, class Iterator, class Attribute>
  auto print_left(Iterator& out, const Attribute& a) const {
    return lhs_.print(out, a); // recurse
  }

  template <choice_printer Left, class Iterator>
  auto print_left(Iterator& out, unused_type) const {
    return lhs_.print(out, unused);
  }

  template <class Left, class Iterator, class Attribute>
    requires(!choice_printer<Left>)
  auto print_left(Iterator& out, const Attribute& a) const {
    auto x = try_as<lhs_attribute>(&a);
    return x && lhs_.print(out, *x);
  }

  template <class Iterator>
  bool print_right(Iterator& out, unused_type) const {
    return rhs_.print(out, unused);
  }

  template <class Iterator, class Attribute>
  auto print_right(Iterator& out, const Attribute& a) const {
    auto x = try_as<rhs_attribute>(&a);
    return x && rhs_.print(out, *x);
  }

  Lhs lhs_;
  Rhs rhs_;
};

} // namespace tenzir
