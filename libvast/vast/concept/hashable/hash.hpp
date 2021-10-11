//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/hashable/concepts.hpp"
#include "vast/concept/hashable/contiguously_hashable.hpp"
#include "vast/concept/hashable/default_hash.hpp"
#include "vast/concept/hashable/hash_append.hpp"

#include <utility>

namespace vast {

/// Generic function to compute a hash over a byte sequence.
/// @param x The value to hash.
/// @param args Optional arguments to seed `HashAlgorithm`.
/// @returns A hash digest of *bytes* using `HashAlgorithm`.
template <incremental_hash HashAlgorithm = default_hash, class T, class... Args>
  requires(
    !contiguously_hashable<T, HashAlgorithm> || !one_shot_hash<HashAlgorithm>)
[[nodiscard]] auto hash(T&& x, Args&&... args) noexcept {
  HashAlgorithm h{std::forward<Args>(args)...};
  hash_append(h, x);
  return static_cast<typename HashAlgorithm::result_type>(h);
}

template <one_shot_hash HashAlgorithm = default_hash, class T, class... Args>
  requires(contiguously_hashable<T, HashAlgorithm>)
[[nodiscard]] auto hash(T&& x, Args&&... args) noexcept {
  HashAlgorithm h{std::forward<Args>(args)...};
  return h.make(std::addressof(x), sizeof(x));
}

} // namespace vast
