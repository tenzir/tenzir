//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concepts.hpp"
#include "tenzir/hash/uniquely_hashable.hpp"

#include <cstddef>
#include <span>
#include <utility>

namespace tenzir {

/// A hash algorithm that supports incremental computation of a hash digest
/// in a construct-add-finish manner.
template <class HashAlgorithm>
concept incremental_hash
  = requires(HashAlgorithm& h, std::span<const std::byte> bytes) {
      // clang-format off
  typename HashAlgorithm::result_type;
  { h.add(bytes) } noexcept -> std::same_as<void>;
  { h.finish() } noexcept
    -> std::same_as<typename HashAlgorithm::result_type>;
      // clang-format on
    };

/// An incremental hash algorithm that can be reset to its initial state.
template <class HashAlgorithm>
concept reusable_hash
  = incremental_hash<HashAlgorithm> and requires(HashAlgorithm& h) {
      // clang-format off
      { h.reset() } noexcept -> std::same_as<void>;
      // clang-format on
    };

/// A hash algorithm that exposes a one-shot computation of a hash digest over
/// a byte sequence.
template <class HashAlgorithm>
concept oneshot_hash = requires(std::span<const std::byte> bytes) {
  // clang-format off
  typename HashAlgorithm::result_type;
  { HashAlgorithm::make(bytes) } noexcept
    -> std::same_as<typename HashAlgorithm::result_type>;
  // clang-format on
};

/// The hash algorithm concept. A hash algorithm can be *oneshot*,
/// *incremental*, or both.
template <class HashAlgorithm>
concept hash_algorithm
  = incremental_hash<HashAlgorithm> or oneshot_hash<HashAlgorithm>;

} // namespace tenzir
