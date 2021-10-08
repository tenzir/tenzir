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
namespace detail {

/// Wraps a one-shot hash in the interface of an incremental hash algorithm.
template <one_shot_hash HashAlgorithm>
class one_shot_wrapper {
public:
  static constexpr detail::endian endian = HashAlgorithm::endian;

  using result_type = typename HashAlgorithm::result_type;

  template <class... Args>
  explicit one_shot_wrapper(Args&&... args) : h_{std::forward<Args>(args)...} {
  }

  void operator()(const void* data, size_t size) noexcept {
    // Guaranteed to be called exactly once, via contiguously_hashable.
    result_ = h_.make(data, size);
  }

  explicit operator result_type() noexcept {
    return result_;
  }

private:
  HashAlgorithm h_;
  result_type result_;
};

} // namespace detail

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
  detail::one_shot_wrapper<HashAlgorithm> h{std::forward<Args>(args)...};
  hash_append(h, x);
  return static_cast<typename HashAlgorithm::result_type>(h);
}

} // namespace vast
