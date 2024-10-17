//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/inspection_common.hpp"
#include "tenzir/detail/type_traits.hpp"

#include <concepts>
#include <cstddef>
#include <iterator>
#include <type_traits>

namespace tenzir::concepts {

template <class T, class... Us>
concept sameish = (std::same_as<std::decay_t<T>, std::decay_t<Us>> or ...);

template <class T>
concept transparent = requires { typename T::is_transparent; };

/// Types that work with std::data and std::size (= containers)
template <class T>
concept container = requires(T& t) {
  std::data(t);
  std::size(t);
};

/// Contiguous byte buffers
template <class T>
concept byte_container = requires(T& t) {
  requires container<T>;
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
  requires byte_sequence<T>;
  requires decltype(as_bytes(x))::extent == std::dynamic_extent;
};

/// A byte sequence that has a fixed number of bytes.
template <class T>
concept fixed_byte_sequence = requires(T& x) {
  requires byte_sequence<T>;
  requires decltype(as_bytes(x))::extent > 0;
  requires decltype(as_bytes(x))::extent != std::dynamic_extent;
};

template <class T>
concept arithmetic = std::is_arithmetic_v<T>;

template <class T>
concept integer
  = std::integral<T> and not sameish<T, bool, char, signed char, unsigned char>;

template <class T>
concept number = integer<T> or std::floating_point<T>;

struct example_inspector {
  static constexpr bool is_loading = true;

  detail::inspection_object<example_inspector> object(auto&);

  bool apply(auto&);

  template <class T>
  detail::inspection_field<T> field(std::string_view, T&);
};

/// Inspectables
template <class T>
concept inspectable = requires(example_inspector& i, T& x) {
  { inspect(i, x) } -> std::same_as<bool>;
};

template <class C>
concept insertable
  = requires(C& xs, typename C::value_type x) { xs.insert(x); };

template <class C>
concept appendable
  = requires(C& xs, typename C::value_type x) { xs.push_back(x); };

/// A type `T` is a semigroup if an associative binary function from two values
/// of `T` to another value of `T` exists. We name this function `mappend` in
/// spirit of Haskell's Monoid typeclass because we can't define new operators
/// in C++ and expect to constrain templates with `monoid` more often than with
/// `semigroup`.
/// For all members x, y, z of T:
/// mappend(x, mappend(y, z)) == mappend(mappend(x, y), z)
template <class T>
concept semigroup = requires(const T& x, const T& y) {
  { mappend(x, y) } -> std::same_as<T>;
};

/// A type `T` is a monoid if it is a `semigroup` and a neutral element for the
/// `mappend` function exists. We require the default constructor to produce
/// this neutral element.
/// For all members x of T:
/// mappend(x, T{}) == mappend(T{}, x) == x
template <class T>
concept monoid = semigroup<T> && std::is_default_constructible_v<T>;

template <class T, class... Ts>
concept one_of = (std::same_as<T, Ts> or ...);

} // namespace tenzir::concepts
