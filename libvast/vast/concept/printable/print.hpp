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

#ifndef VAST_CONCEPT_PRINTABLE_PRINT_HPP
#define VAST_CONCEPT_PRINTABLE_PRINT_HPP

#include <type_traits>

#include "vast/access.hpp"
#include "vast/concept/printable/core/printer.hpp"

namespace vast {

template <class Iterator, class T, class... Args>
auto print(Iterator&& out, const T& x, Args&&... args)
  -> std::enable_if_t<has_printer<T>::value, bool> {
  return make_printer<T>{std::forward<Args>(args)...}.print(out, x);
}

template <class Iterator, class T, class... Args>
auto print(Iterator&& out, const T& x, Args&&... args)
  -> std::enable_if_t<!has_printer<T>::value && has_access_printer<T>::value,
                      bool> {
  return access::printer<T>{std::forward<Args>(args)...}.print(out, x);
}

//namespace detail {
//
//template <class Iterator, class T>
//bool conjunctive_print(Iterator& out, const T& x) {
//  return print(out, x);
//}
//
//template <class Iterator, class T, class... Ts>
//bool conjunctive_print(Iterator& out, const T& x, const Ts&... xs) {
//  return conjunctive_print(out, x) && conjunctive_print(out, xs...);
//}
//
//} // namespace detail
//
//template <class Iterator, class T>
//auto print(Iterator&& out, const T& x)
//  -> std::enable_if_t<!has_printer<T>::value && has_access_state<T>::value,
//                      bool> {
//  bool r;
//  auto fun = [&](auto&... xs) { r = detail::conjunctive_print(out, xs...); };
//  access::state<T>::call(x, fun);
//  return r;
//}

namespace detail {

struct is_printable {
  template <class I, class T>
  static auto test(I* out, const T* x) -> decltype(print(*out, *x), std::true_type());

  template <class, class>
  static auto test(...) -> std::false_type;
};

} // namespace detail

template <class I, class T>
struct is_printable : decltype(detail::is_printable::test<I, T>(0, 0)) {};

} // namespace vast

#endif
