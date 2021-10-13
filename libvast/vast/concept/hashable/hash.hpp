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

#include <utility>

namespace vast {

/// Generic function to compute an incremental hash over an instance of a
/// hashable type.
/// @param x The value to hash.
/// @returns A hash digest of *x* using `HashAlgorithm`.
template <incremental_hash HashAlgorithm = default_hash, class T>
  requires(!portable_hash<T, HashAlgorithm> || !oneshot_hash<HashAlgorithm>)
[[nodiscard]] auto hash(T&& x) noexcept {
  HashAlgorithm h{};
  hash_append(h, x);
  return static_cast<typename HashAlgorithm::result_type>(h);
}

/// Generic function to compute a one-shot hash over an instance of a hashable
/// type.
/// @param x The value to hash.
/// @returns A hash digest of *x* using `HashAlgorithm`.
template <oneshot_hash HashAlgorithm = default_hash, class T>
  requires(portable_hash<T, HashAlgorithm>)
[[nodiscard]] auto hash(T&& x) noexcept {
  HashAlgorithm h{};
  return h.make(std::addressof(x), sizeof(x));
}

} // namespace vast
