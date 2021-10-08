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

template <class T, class HashAlgorithm>
struct is_contiguously_hashable
  : std::bool_constant<
      detail::uniquely_represented<
        T> && (sizeof(T) == 1 || HashAlgorithm::endian == detail::endian::native)> {
};

template <class T, size_t N, class HashAlgorithm>
struct is_contiguously_hashable<T[N], HashAlgorithm>
  : std::bool_constant<
      detail::uniquely_represented<
        T[N]> && (sizeof(T) == 1 || HashAlgorithm::endian == detail::endian::native)> {
};

template <class T, class HashAlgorithm>
concept contiguously_hashable
  = is_contiguously_hashable<T, HashAlgorithm>::value;

} // namespace vast
