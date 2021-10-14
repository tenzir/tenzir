//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/detail/bit.hpp"

#include <cstddef>
#include <type_traits>

// From xxhash.h:
// Inlining improves performance on small inputs, especially when the length is
// expressed as a compile-time constant. [..]. It also keeps xxHash symbols
// private to the unit, so they are not exported.
#define XXH_INLINE_ALL

#include <xxhash.h>

namespace vast {

class xxh64 {
public:
  static constexpr detail::endian endian
    = XXH_CPU_LITTLE_ENDIAN ? detail::endian::little : detail::endian::big;

  using result_type = XXH64_hash_t;
  using seed_type = XXH64_hash_t;

  static result_type
  make(const void* data, size_t size, seed_type seed = 0) noexcept {
    return XXH64(data, size, seed);
  }

  explicit xxh64(seed_type seed = 0) noexcept {
    XXH64_reset(&state_, seed);
  }

  void operator()(const void* data, size_t size) noexcept {
    XXH64_update(&state_, data, size);
  }

  explicit operator result_type() noexcept {
    return XXH64_digest(&state_);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, xxh64& x) {
    return f(x.state_);
  }

private:
  XXH64_state_t state_;
};

class xxh3_64 {
public:
  static constexpr detail::endian endian = detail::endian::native;

  using result_type = XXH64_hash_t;
  using seed_type = XXH64_hash_t;

  static result_type make(const void* data, size_t size) noexcept {
    return XXH3_64bits(data, size);
  }

  static result_type
  make(const void* data, size_t size, seed_type seed) noexcept {
    return XXH3_64bits_withSeed(data, size, seed);
  }

  xxh3_64() noexcept {
    XXH3_INITSTATE(&state_);
    XXH3_64bits_reset(&state_);
  }

  explicit xxh3_64(seed_type seed) noexcept {
    std::memset(&state_, 0, sizeof(state_));
    XXH3_64bits_reset_withSeed(&state_, seed);
  }

  void operator()(const void* data, size_t size) noexcept {
    XXH3_64bits_update(&state_, data, size);
  }

  explicit operator result_type() noexcept {
    return XXH3_64bits_digest(&state_);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, xxh3_64& x) {
    return f(x.state_);
  }

private:
  XXH3_state_t state_;
};

class xxh3_128 {
public:
  static constexpr detail::endian endian = detail::endian::native;

  using result_type = XXH128_hash_t;
  using seed_type = XXH64_hash_t;

  static result_type make(const void* data, size_t size) noexcept {
    return XXH3_128bits(data, size);
  }

  static result_type
  make(const void* data, size_t size, seed_type seed) noexcept {
    return XXH3_128bits_withSeed(data, size, seed);
  }

  xxh3_128() noexcept {
    XXH3_INITSTATE(&state_);
    XXH3_128bits_reset(&state_);
  }

  explicit xxh3_128(seed_type seed) noexcept {
    std::memset(&state_, 0, sizeof(state_));
    XXH3_128bits_reset_withSeed(&state_, seed);
  }

  void operator()(const void* data, size_t size) noexcept {
    XXH3_128bits_update(&state_, data, size);
  }

  explicit operator result_type() noexcept {
    return XXH3_128bits_digest(&state_);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, xxh3_128& x) {
    return f(x.state_);
  }

private:
  XXH3_state_t state_;
};

} // namespace vast
