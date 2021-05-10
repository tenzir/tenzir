//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/support/detail/attr_fold.hpp"
#include "vast/detail/type_traits.hpp"

#include <type_traits>
#include <vector>

namespace vast {
namespace detail {

template <class Elem>
struct container {
  using vector_type = std::vector<Elem>;
  using attribute = attr_fold_t<vector_type>;

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
    c.insert(c.end(), std::forward<T>(x));
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

