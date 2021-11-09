//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concepts.hpp"
#include "vast/hash/uniquely_hashable.hpp"

#include <cstddef>
#include <span>
#include <utility>

namespace vast {

/// A hash algorithm that supports incremental computation of a hash digest
/// in a construct-add-finish manner.
template <class HashAlgorithm>
concept incremental_hash
  = requires(HashAlgorithm& h, std::span<const std::byte> bytes) {
  // clang-format off
  typename HashAlgorithm::result_type;
  { h.add(bytes) } noexcept -> concepts::same_as<void>;
  { h.finish() } noexcept 
    -> concepts::same_as<typename HashAlgorithm::result_type>;
  // clang-format on
};

/// A hash algorithm that exposes a one-shot computation of a hash digest over
/// a byte sequence.
template <class HashAlgorithm>
concept oneshot_hash = requires(std::span<const std::byte> bytes) {
  // clang-format off
  typename HashAlgorithm::result_type;
  { HashAlgorithm::make(bytes) } noexcept
    -> concepts::same_as<typename HashAlgorithm::result_type>;
  // clang-format on
};

/// The hash algorithm concept. A hash algorithm can be *oneshot*,
/// *incremental*, or both.
template <class HashAlgorithm>
concept hash_algorithm
  = incremental_hash<HashAlgorithm> || oneshot_hash<HashAlgorithm>;

} // namespace vast
