//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/hashable/concepts.hpp"
#include "vast/concept/hashable/default_hash.hpp"
#include "vast/concept/hashable/hash_append.hpp"
#include "vast/concept/hashable/portable_hash.hpp"
#include "vast/concepts.hpp"

#include <utility>

namespace vast {

/// Generic function to compute a hash diegst over a hashable type. The
/// implementation chooses the fastest possible way to compute the hash.
/// @tparam The hash algorithm to use for hashing.
/// @param x The value to hash.
/// @returns A hash digest of *x* using `HashAlgorithm`.
template <hash_algorithm HashAlgorithm = default_hash, class T>
[[nodiscard]] auto hash(const T& x) noexcept {
  constexpr auto is_oneshot = oneshot_hash<HashAlgorithm>;
  if constexpr (is_oneshot && portable_hash<T, HashAlgorithm>) {
    return HashAlgorithm::make(std::addressof(x), sizeof(x));
  } else if constexpr (is_oneshot && concepts::fixed_byte_sequence<T>) {
    auto bytes = as_bytes(x);
    return HashAlgorithm::make(bytes.data(), bytes.size());
  } else if constexpr (incremental_hash<HashAlgorithm>) {
    HashAlgorithm h{};
    hash_append(h, x);
    return static_cast<typename HashAlgorithm::result_type>(h);
  }
}

} // namespace vast
