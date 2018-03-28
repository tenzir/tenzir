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

#include "vast/access.hpp"
#include "vast/concept/parseable/core/parser.hpp"

namespace vast {

template <class Iterator, class T, class... Args>
auto parse(Iterator& f, const Iterator& l, T& x, Args&&... args)
  -> std::enable_if_t<has_parser<T>::value, bool> {
  return make_parser<T>{std::forward<Args>(args)...}(f, l, x);
}

template <class Iterator, class T, class... Args>
auto parse(Iterator& f, const Iterator& l, T& x, Args&&... args)
  -> std::enable_if_t<!has_parser<T>::value && has_access_parser<T>::value,
                      bool> {
  return access::parser<T>{std::forward<Args>(args)...}(f, l, x);
}

namespace detail {

template <class Iterator, class T>
bool conjunctive_parse(Iterator& f, const Iterator& l, T& x) {
  return parse(f, l, x);
}

template <class Iterator, class T, class... Ts>
bool conjunctive_parse(Iterator& f, const Iterator& l, T& x, Ts&... xs) {
  return conjunctive_parse(f, l, x) && conjunctive_parse(f, l, xs...);
}

} // namespace detail

template <class Iterator, class T>
auto parse(Iterator& f, const Iterator& l, T& x)
  -> std::enable_if_t<!has_parser<T>::value && has_access_state<T>::value,
                      bool> {
  bool r;
  auto fun = [&](auto&... xs) { r = detail::conjunctive_parse(f, l, xs...); };
  access::state<T>::call(x, fun);
  return r;
}

namespace detail {

struct is_parseable {
  template <class I, class T>
  static auto test(I* f, const I* l, T* x)
    -> decltype(parse(*f, *l, *x), std::true_type());

  template <class, class>
  static auto test(...) -> std::false_type;
};

} // namespace detail

template <class I, class T>
struct is_parseable : decltype(detail::is_parseable::test<I, T>(0, 0, 0)) {};

} // namespace vast

