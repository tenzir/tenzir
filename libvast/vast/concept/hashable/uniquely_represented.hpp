//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/type_traits.hpp"

#include <array>
#include <tuple>
#include <type_traits>

namespace vast::detail {

// The `is_uniquely_representable` trait is fulfilled when the hash of the
// object is exactly the hash of the memory region of the object regarded as an
// opaque byte array, with no holes or padding bytes.

template <class T>
struct is_uniquely_represented
  : std::bool_constant<std::is_integral<T>{} || std::is_enum<T>{}
                       || std::is_pointer<T>{}> {};

template <class T>
struct is_uniquely_represented<T const> : is_uniquely_represented<T> {};

template <class T, class U>
struct is_uniquely_represented<std::pair<T, U>>
  : std::bool_constant<is_uniquely_represented<T>{}
                       && is_uniquely_represented<U>{}
                       && sizeof(T) + sizeof(U) == sizeof(std::pair<T, U>)> {};

template <class... T>
struct is_uniquely_represented<std::tuple<T...>>
  : std::bool_constant<
      std::conjunction_v<is_uniquely_represented<
        T>...> && sum<sizeof(T)...> == sizeof(std::tuple<T...>)> {};

template <class T, size_t N>
struct is_uniquely_represented<T[N]> : is_uniquely_represented<T> {};

template <class T, size_t N>
struct is_uniquely_represented<std::array<T, N>>
  : std::bool_constant<is_uniquely_represented<T>{}
                       && sizeof(T) * N == sizeof(std::array<T, N>)> {};

template <class T>
concept uniquely_represented = is_uniquely_represented<T>::value;

} // namespace vast::detail
