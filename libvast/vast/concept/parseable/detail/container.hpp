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

#include <vector>
#include <type_traits>

#include "vast/concept/support/detail/attr_fold.hpp"

namespace vast {
namespace detail {

template <class T>
struct is_pair : std::false_type {};

template <class T, class U>
struct is_pair<std::pair<T, U>> : std::true_type {};

template <class T>
constexpr bool is_pair_v = is_pair<T>::value;

template <class Elem>
struct container {
  using vector_type = std::vector<Elem>;
  using attribute = typename attr_fold<vector_type>::type;

  template <class T>
  struct lazy_value_type {
    using value_type = T;
  };

  using value_type =
    typename std::conditional_t<
      std::is_same<attribute, std::decay_t<unused_type>>{},
      lazy_value_type<unused_type>,
      attribute
    >::value_type;

  static constexpr bool modified = std::is_same_v<vector_type, attribute>;

  template <class Container, class T>
  static void push_back(Container& c, T&& x) {
    c.insert(c.end(), std::move(x));
  }

  template <class Container>
  static void push_back(Container&, unused_type) {
    // nop
  }

  template <class T>
  static void push_back(unused_type, T&&) {
    // nop
  }

  template <class Parser, class Iterator>
  static bool parse(const Parser& p, Iterator& f, const Iterator& l,
                    unused_type) {
    return p(f, l, unused);
  }

  template <class Parser, class Iterator, class Attribute>
  static bool parse(const Parser& p, Iterator& f, const Iterator& l,
                    Attribute& a) {
    if constexpr (!is_pair_v<typename Attribute::value_type>) {
      value_type x;
      if (!p(f, l, x))
        return false;
      push_back(a, std::move(x));
    } else {
      using pair_type =
        std::pair<
          std::remove_const_t<typename Attribute::value_type::first_type>,
          typename Attribute::value_type::second_type
        >;
      pair_type pair;
      if (!p(f, l, pair))
        return false;
      push_back(a, std::move(pair));
    }
    return true;
  }
};

} // namespace detail
} // namespace vast

