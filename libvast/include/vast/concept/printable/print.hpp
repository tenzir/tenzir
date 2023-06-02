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

template <class Iterator, registered_printer T, class... Args>
auto print(Iterator&& out, const T& x, Args&&... args) {
  return make_printer<T>{std::forward<Args>(args)...}.print(out, x);
}

template <class Iterator, access_printer T, class... Args>
  requires(!registered_printer<T>)
auto print(Iterator&& out, const T& x, Args&&... args) {
  return access::printer<T>{std::forward<Args>(args)...}.print(out, x);
}

// namespace detail {
//
// template <class Iterator, class T>
// bool conjunctive_print(Iterator& out, const T& x) {
//  return print(out, x);
//}
//
// template <class Iterator, class T, class... Ts>
// bool conjunctive_print(Iterator& out, const T& x, const Ts&... xs) {
//  return conjunctive_print(out, x) && conjunctive_print(out, xs...);
//}
//
//} // namespace detail
//
// template <class Iterator, class T>
// auto print(Iterator&& out, const T& x)
//  -> std::enable_if_t<!printer<T>&& has_access_state<T>::value,
//                      bool> {
//  bool r;
//  auto fun = [&](auto&... xs) { r = detail::conjunctive_print(out, xs...); };
//  access::state<T>::call(x, fun);
//  return r;
//}

template <class Iterator, class T>
concept printable = requires(Iterator out, T x) { print(out, x); };

template <class Iterator, class T>
using is_printable = std::bool_constant<printable<Iterator, T>>;

} // namespace vast
