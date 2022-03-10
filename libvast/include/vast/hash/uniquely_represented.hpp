//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <array>
#include <cstddef>
#include <tuple>
#include <type_traits>

namespace vast {

// A type is *uniquely representable* if and only if the hash digest of the
// object is equal to the memory region of the object when interpreted as
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

template <class... Ts>
struct is_uniquely_represented<std::tuple<Ts...>>
  : std::bool_constant<(is_uniquely_represented<Ts>{} && ...)
                       && ((0 + ... + sizeof(Ts)) == sizeof(std::tuple<Ts...>))> {
};

template <class T, size_t N>
struct is_uniquely_represented<T[N]> : is_uniquely_represented<T> {};

template <class T, size_t N>
struct is_uniquely_represented<std::array<T, N>>
  : std::bool_constant<is_uniquely_represented<T>{}
                       && sizeof(T) * N == sizeof(std::array<T, N>)> {};

template <class T>
concept uniquely_represented = is_uniquely_represented<T>::value;

} // namespace vast
