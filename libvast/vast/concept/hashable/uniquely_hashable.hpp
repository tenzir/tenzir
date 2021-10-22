//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/hashable/uniquely_represented.hpp"
#include "vast/concepts.hpp"
#include "vast/detail/bit.hpp"

#include <type_traits>

namespace vast {

// A type `T` produces a platform-independent unique hash digest under a hash
// algorithm `H` if (i) it fulfils the concept `uniquely_represented<T>` and
// (ii) the endianness of `H` equals to the host endian. All (fixed) byte
// sequences are uniquely represented by definition.

// clang-format off
template <class T, class HashAlgorithm>
struct is_uniquely_hashable
  : std::bool_constant<
      concepts::fixed_byte_sequence<T>
      || (uniquely_represented<T>
          && (sizeof(T) == 1
              || HashAlgorithm::endian == detail::endian::native))
    > {};
// clang-format on

template <class T, size_t N, class HashAlgorithm>
struct is_uniquely_hashable<T[N], HashAlgorithm>
  : std::bool_constant<
      uniquely_represented<
        T[N]> && (sizeof(T) == 1 || HashAlgorithm::endian == detail::endian::native)> {
};

template <class T, class HashAlgorithm>
concept uniquely_hashable = is_uniquely_hashable<T, HashAlgorithm>::value;

} // namespace vast
