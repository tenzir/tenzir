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

template <typename Iterator, typename T, typename... Args>
auto print(Iterator&& out, T const& x, Args&&... args)
  -> std::enable_if_t<has_printer<T>::value, bool> {
  return make_printer<T>{std::forward<Args>(args)...}.print(out, x);
}

template <typename Iterator, typename T, typename... Args>
auto print(Iterator&& out, T const& x, Args&&... args)
  -> std::enable_if_t<!has_printer<T>::value && has_access_printer<T>::value,
                      bool> {
  return access::printer<T>{std::forward<Args>(args)...}.print(out, x);
}

//namespace detail {
//
//template <typename Iterator, typename T>
//bool conjunctive_print(Iterator& out, T const& x) {
//  return print(out, x);
//}
//
//template <typename Iterator, typename T, typename... Ts>
//bool conjunctive_print(Iterator& out, T const& x, Ts const&... xs) {
//  return conjunctive_print(out, x) && conjunctive_print(out, xs...);
//}
//
//} // namespace detail
//
//template <typename Iterator, typename T>
//auto print(Iterator&& out, T const& x)
//  -> std::enable_if_t<!has_printer<T>::value && has_access_state<T>::value,
//                      bool> {
//  bool r;
//  auto fun = [&](auto&... xs) { r = detail::conjunctive_print(out, xs...); };
//  access::state<T>::call(x, fun);
//  return r;
//}

namespace detail {

struct is_printable {
  template <typename I, typename T>
  static auto test(I* out, T const* x) -> decltype(print(*out, *x), std::true_type());

  template <typename, typename>
  static auto test(...) -> std::false_type;
};

} // namespace detail

template <typename I, typename T>
struct is_printable : decltype(detail::is_printable::test<I, T>(0, 0)) {};

} // namespace vast

#endif
