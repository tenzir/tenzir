//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"
#include "tenzir/hash/uniquely_represented.hpp"

#include <bit>
#include <type_traits>

namespace tenzir {

// A type `T` produces a platform-independent unique hash digest under a hash
// algorithm `H` if (i) it fulfils the concept `uniquely_represented<T>` and
// (ii) the endianness of `H` equals to the host endian. All (fixed) byte
// sequences are uniquely represented by definition.

template <class T, class HashAlgorithm>
struct is_uniquely_hashable
  : std::bool_constant<
      concepts::fixed_byte_sequence<T>
      or (uniquely_represented<T>
          and (sizeof(T) == 1 or HashAlgorithm::endian == std::endian::native))> {
};

template <class T, size_t N, class HashAlgorithm>
struct is_uniquely_hashable<T[N], HashAlgorithm>
  : std::bool_constant<uniquely_represented<T[N]>
                       and (sizeof(T) == 1
                            or HashAlgorithm::endian == std::endian::native)> {
};

template <class T, class HashAlgorithm>
concept uniquely_hashable = is_uniquely_hashable<T, HashAlgorithm>::value;

} // namespace tenzir
