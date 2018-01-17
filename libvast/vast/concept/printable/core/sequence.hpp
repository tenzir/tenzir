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

#ifndef VAST_CONCEPT_PRINTABLE_CORE_SEQUENCE_HPP
#define VAST_CONCEPT_PRINTABLE_CORE_SEQUENCE_HPP

#include <tuple>
#include <type_traits>

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/support/detail/attr_fold.hpp"

#include "vast/detail/type_traits.hpp"

namespace vast {

template <typename Lhs, typename Rhs>
class sequence_printer;

template <typename>
struct is_sequence_printer : std::false_type {};

template <typename... Ts>
struct is_sequence_printer<sequence_printer<Ts...>> : std::true_type {};

// TODO: factor helper functions shared among sequence printer and parser.

template <typename Lhs, typename Rhs>
class sequence_printer : public printer<sequence_printer<Lhs, Rhs>> {
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
      std::is_same<lhs_attribute, unused_type>{}
        && std::is_same<rhs_attribute, unused_type>{},
      unused_type,
      std::conditional_t<
        std::is_same<lhs_attribute, unused_type>{},
        rhs_attribute,
        std::conditional_t<
          std::is_same<rhs_attribute, unused_type>{},
          lhs_attribute,
          typename detail::attr_fold<
            decltype(std::tuple_cat(detail::tuple_wrap<lhs_attribute>{},
                                    detail::tuple_wrap<rhs_attribute>{}))
          >::type
        >
      >
    >;

  sequence_printer(Lhs lhs, Rhs rhs)
    : lhs_{std::move(lhs)}, rhs_{std::move(rhs)} {
  }

  template <typename Iterator, typename Attribute>
  bool print(Iterator& out, Attribute const& a) const {
    return print_left(out, a) && print_right(out, a);
  }

private:
  template <typename T>
  static constexpr auto depth_helper()
  -> std::enable_if_t<!is_sequence_printer<T>::value, size_t> {
    return 0;
  }

  template <typename T>
  static constexpr auto depth_helper()
  -> std::enable_if_t<
       is_sequence_printer<T>::value
        && (std::is_same<typename T::lhs_attribute, unused_type>::value
            || std::is_same<typename T::rhs_attribute, unused_type>::value),
       size_t
     > {
    return depth_helper<typename T::lhs_type>();
  }

  template <typename T>
  static constexpr auto depth_helper()
  -> std::enable_if_t<
       is_sequence_printer<T>::value
        && ! std::is_same<typename T::lhs_attribute, unused_type>::value
        && ! std::is_same<typename T::rhs_attribute, unused_type>::value,
       size_t
     > {
    return 1 + depth_helper<typename T::lhs_type>();
  }

  static constexpr size_t depth() {
    return depth_helper<sequence_printer>();
  }

  template <typename L, typename T>
  static auto get_helper(T const& x)
  -> std::enable_if_t<is_sequence_printer<L>{}, T const&> {
    return x;
  }

  template <typename L, typename T>
  static auto get_helper(T const& x)
  -> std::enable_if_t<
       ! is_sequence_printer<L>::value,
       decltype(std::get<0>(x))
     > {
    return std::get<0>(x);
  }

  template <typename Iterator, typename... Ts>
  bool print_left(Iterator& out, std::tuple<Ts...> const& t) const {
    return lhs_.print(out, get_helper<lhs_type>(t));
  }

  template <typename Iterator, typename... Ts>
  bool print_right(Iterator& out, std::tuple<Ts...> const& t) const {
    return rhs_.print(out, std::get<depth()>(t));
  }

  template <typename Iterator>
  bool print_left(Iterator& out, unused_type) const {
    return lhs_.print(out, unused);
  }

  template <typename Iterator>
  bool print_right(Iterator& out, unused_type) const {
    return rhs_.print(out, unused);
  }

  template <typename Iterator, typename T, typename U>
  bool print_left(Iterator& out, std::pair<T, U> const& p) const {
    return lhs_.print(out, p.first);
  }

  template <typename Iterator, typename T, typename U>
  bool print_right(Iterator& out, std::pair<T, U> const& p) const {
    return rhs_.print(out, p.second);
  }

  template <typename Iterator, typename Attribute>
  bool print_left(Iterator& out, Attribute const& a) const {
    return lhs_.print(out, a);
  }

  template <typename Iterator, typename Attribute>
  bool print_right(Iterator& out, Attribute const& a) const {
    return rhs_.print(out, a);
  }

  Lhs lhs_;
  Rhs rhs_;
};

} // namespace vast

#endif
