//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/hashable/uniquely_represented.hpp"
#include "vast/detail/bit.hpp"

#include <type_traits>

namespace vast {

// A type `T` produces a platform-independent (portable) hash digest under a
// hash algorithm `H` if (i) it fulfils the concept `uniquely_represented<T>`
// and (ii) the endianness of `H` equals to the host endian.

template <class T, class HashAlgorithm>
struct has_portable_hash
  : std::bool_constant<
      uniquely_represented<
        T> && (sizeof(T) == 1 || HashAlgorithm::endian == detail::endian::native)> {
};

template <class T, size_t N, class HashAlgorithm>
struct has_portable_hash<T[N], HashAlgorithm>
  : std::bool_constant<
      uniquely_represented<
        T[N]> && (sizeof(T) == 1 || HashAlgorithm::endian == detail::endian::native)> {
};

template <class T, class HashAlgorithm>
concept portable_hash = has_portable_hash<T, HashAlgorithm>::value;

} // namespace vast
