//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/hashable/uniquely_hashable.hpp"
#include "vast/concepts.hpp"

#include <cstddef>
#include <span>
#include <utility>

namespace vast {

/// A hash algorithm that supports incremental computation of a hash digest
/// in a construct-add-finish manner.
template <class HashAlgorithm>
concept incremental_hash = requires(HashAlgorithm& h, const void* p, size_t n) {
  // clang-format off
  typename HashAlgorithm::result_type;
  { h(p, n) } noexcept -> concepts::same_as<void>;
  static_cast<typename HashAlgorithm::result_type>(h);
  // clang-format on
};

/// A hash algorithm that exposes a one-shot computation of a hash digest over
/// a byte sequence.
template <class HashAlgorithm>
concept oneshot_hash = requires(const void* p, size_t n) {
  // clang-format off
  typename HashAlgorithm::result_type;
  { HashAlgorithm::make(p, n) } noexcept
    -> concepts::same_as<typename HashAlgorithm::result_type>;
  // clang-format on
};

/// The hash algorithm concept. A hash algorithm can be *oneshot*,
/// *incremental*, or both.
template <class HashAlgorithm>
concept hash_algorithm
  = incremental_hash<HashAlgorithm> || oneshot_hash<HashAlgorithm>;

/// Checks whether a type is oneshot hashable with a given hash algorithm.
template <class T, class HashAlgorithm>
concept oneshot_hashable = oneshot_hash<HashAlgorithm> &&(
  uniquely_hashable<T, HashAlgorithm> || concepts::fixed_byte_sequence<T>);

} // namespace vast
