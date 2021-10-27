//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/type_traits.hpp"

#include <cstddef>
#include <iterator>
#include <type_traits>

namespace vast::concepts {

template <class T, class U>
concept SameHelper = std::is_same_v<T, U>;

template <class T, class U>
concept same_as = SameHelper<T, U> && SameHelper<U, T>;

template <class T, class U>
concept sameish = same_as<std::decay_t<T>, std::decay_t<U>>;

template <class T, class U>
concept different = !same_as<T, U>;

template <typename From, typename To>
concept convertible_to = std::is_convertible_v<From, To> && requires(
  std::add_rvalue_reference_t<From> (&f)()) {
  static_cast<To>(f());
};

template <class T>
concept transparent = requires {
  typename T::is_transparent;
};

// Replace this with std::ranges::range concept once all compilers support
// <ranges> header
template <class T>
concept range = requires(T& t) {
  std::begin(t);
  std::end(t);
};

/// Types that work with std::data and std::size (= containers)
template <class T>
concept container = requires(T& t) {
  std::data(t);
  std::size(t);
};

/// Contiguous byte buffers
template <class T>
concept byte_container = requires(T& t) {
  container<T>;
  requires sizeof(decltype(*std::data(t))) == 1;
};

/// A type that can be interpreted as sequence of bytes.
template <class T>
concept byte_sequence = requires(T& x) {
  requires detail::is_span_v<decltype(as_bytes(x))>;
  requires std::is_same_v<typename decltype(as_bytes(x))::element_type,
                          const std::byte>;
};

/// A byte sequence that has a variable number of bytes.
template <class T>
concept variable_byte_sequence = requires(T& x) {
  byte_sequence<T>;
  requires decltype(as_bytes(x))::extent == std::dynamic_extent;
};

/// A byte sequence that has a fixed number of bytes.
template <class T>
concept fixed_byte_sequence = requires(T& x) {
  byte_sequence<T>;
  requires decltype(as_bytes(x))::extent > 0;
  requires decltype(as_bytes(x))::extent != std::dynamic_extent;
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
  using result_type = void;
  template <class... Ts>
  void operator()(Ts&&...);
};

/// Inspectables
template <class T>
concept inspectable = requires(any_callable& i, T& x) {
  {inspect(i, x)};
};

template <class C>
concept insertable = requires(C& xs, typename C::value_type x) {
  xs.insert(x);
};

template <class C>
concept appendable = requires(C& xs, typename C::value_type x) {
  xs.push_back(x);
};

/// A type `T` is a semigroup if an associative binary function from two values
/// of `T` to another value of `T` exists. We name this function `mappend` in
/// spirit of Haskell's Monoid typeclass because we can't define new operators
/// in C++ and expect to constrain templates with `monoid` more often than with
/// `semigroup`.
/// For all members x, y, z of T:
/// mappend(x, mappend(y, z)) == mappend(mappend(x, y), z)
template <class T>
concept semigroup = requires(const T& x, const T& y) {
  { mappend(x, y) } -> same_as<T>;
};

/// A type `T` is a monoid if it is a `semigroup` and a neutral element for the
/// `mappend` function exists. We require the default constructor to produce
/// this neutral element.
/// For all members x of T:
/// mappend(x, T{}) == mappend(T{}, x) == x
template <class T>
concept monoid = semigroup<T> && std::is_default_constructible_v<T>;

} // namespace vast::concepts
