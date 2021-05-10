//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/access.hpp"
#include "vast/concept/printable/core/printer.hpp"

#include <type_traits>

namespace vast {

template <class Iterator, class T, class... Args>
auto print(Iterator&& out, const T& x, Args&&... args)
  -> std::enable_if_t<has_printer_v<T>, bool> {
  return make_printer<T>{std::forward<Args>(args)...}.print(out, x);
}

template <class Iterator, class T, class... Args>
auto print(Iterator&& out, const T& x, Args&&... args)
  -> std::enable_if_t<!has_printer_v<T> && has_access_printer_v<T>,
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

template <class I, class T>
constexpr bool is_printable_v = is_printable<I, T>::value;

} // namespace vast
