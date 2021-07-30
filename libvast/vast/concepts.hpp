//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/type_traits.hpp"

#include <iterator>
#include <type_traits>

namespace vast::concepts {

template <class T, class U>
concept SameHelper = std::is_same_v<T, U>;

template <class T, class U>
concept same_as = SameHelper<T, U> && SameHelper<U, T>;

template <typename From, typename To>
concept convertible_to = std::is_convertible_v<From, To> && requires(
  std::add_rvalue_reference_t<From> (&f)()) {
  static_cast<To>(f());
};

template <class T>
concept transparent = requires {
  typename T::is_transparent;
};

/// Types that work with std::data and std::size (= containers)
template <class T>
concept container = requires(T t) {
  std::data(t);
  std::size(t);
};

/// Contiguous byte buffers
template <class T>
concept byte_container = requires(T t) {
  std::data(t);
  std::size(t);
  sizeof(decltype(*std::data(t))) == 1;
};

template <class T>
concept integral = std::is_integral_v<T>;

template <class T>
concept unsigned_integral = integral<T> && std::is_unsigned_v<T>;

template <class T>
concept signed_integral = integral<T> && std::is_signed_v<T>;

template <class T>
concept floating_point = std::is_floating_point_v<T>;

struct any_callable {
  template <class... Ts>
  void operator()(Ts&&...);
};

/// Inspectables
template <class T>
concept inspectable = requires(any_callable& i, T& x) {
  {inspect(i, x)};
};

template <class C>
concept insertable = requires(C xs, typename C::value_type x) {
  xs.insert(x);
};

template <class C>
concept appendable = requires(C xs, typename C::value_type x) {
  xs.push_back(x);
};

template <class T>
concept monoid = requires(T x, T y) {
  { mappend(x, y) } -> same_as<T>;
};

} // namespace vast::concepts
