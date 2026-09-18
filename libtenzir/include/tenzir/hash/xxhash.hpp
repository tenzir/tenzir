//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"

#include <bit>
#include <cstddef>
#include <cstring>
#include <type_traits>

// Exposes the layout of `XXH64_state_t`, which we hold by value below. Its
// layout has been ABI-stable since xxhash 0.7.
#define XXH_STATIC_LINKING_ONLY
#include <xxhash.h>

namespace tenzir {

// Exposed xxHash tuning parameters. These mirror xxhash's own defaults
// (see xxhash.h's `XXH_FORCE_ALIGN_CHECK`/`XXH_CPU_LITTLE_ENDIAN` docs) rather
// than reading xxhash's internal macros, which are only defined when
// xxhash.h's implementation section is visible (e.g. under `XXH_INLINE_ALL`).

/// Use special path for aligned inputs (XXH32 and XXH64 only).
static constexpr bool xxh_force_align_check = false;

/// Use fast-path for aligned read at the cost of one branch per hash.
static constexpr int xxh_force_memory_access = 0;

class xxh64 {
public:
  static constexpr std::endian endian = std::endian::native;

  using result_type = XXH64_hash_t;
  using seed_type = XXH64_hash_t;

  static result_type
  make(std::span<const std::byte> bytes, seed_type seed = 0) noexcept {
    TENZIR_ASSERT(bytes.data() != nullptr or bytes.empty());
    return XXH64(bytes.data(), bytes.size(), seed);
  }

  explicit xxh64(seed_type seed = 0) noexcept : seed_{seed} {
    reset();
  }

  void reset() noexcept {
    XXH64_reset(&state_, seed_);
  }

  void add(std::span<const std::byte> bytes) noexcept {
    TENZIR_ASSERT(bytes.data() != nullptr or bytes.empty());
    // Silence a false positive in the `CI` build configuration.
    TENZIR_DIAGNOSTIC_PUSH
    _Pragma("GCC diagnostic ignored \"-Warray-bounds\"")
      XXH64_update(&state_, bytes.data(), bytes.size());
    TENZIR_DIAGNOSTIC_POP
  }

  result_type finish() noexcept {
    return XXH64_digest(&state_);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, xxh64& x) {
    return f(x.state_);
  }

private:
  XXH64_state_t state_;
  seed_type seed_ = 0;
};

class xxh3_64 {
public:
  static constexpr std::endian endian = std::endian::native;

  using result_type = XXH64_hash_t;
  using seed_type = XXH64_hash_t;

  static result_type make(std::span<const std::byte> bytes) noexcept {
    TENZIR_ASSERT(bytes.data() != nullptr or bytes.empty());
    return XXH3_64bits(bytes.data(), bytes.size());
  }

  static result_type
  make(std::span<const std::byte> bytes, seed_type seed) noexcept {
    TENZIR_ASSERT(bytes.data() != nullptr or bytes.empty());
    return XXH3_64bits_withSeed(bytes.data(), bytes.size(), seed);
  }

  xxh3_64() noexcept : seed_{0} {
    XXH3_INITSTATE(&state_);
    reset();
  }

  explicit xxh3_64(seed_type seed) noexcept : seed_{seed} {
    std::memset(&state_, 0, sizeof(state_));
    reset();
  }

  void reset() noexcept {
    XXH3_64bits_reset_withSeed(&state_, seed_);
  }

  void add(std::span<const std::byte> bytes) noexcept {
    TENZIR_ASSERT(bytes.data() != nullptr or bytes.empty());
    XXH3_64bits_update(&state_, bytes.data(), bytes.size());
  }

  result_type finish() noexcept {
    return XXH3_64bits_digest(&state_);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, xxh3_64& x) {
    return f(x.state_);
  }

private:
  XXH3_state_t state_;
  seed_type seed_ = 0;
};

class xxh3_128 {
public:
  static constexpr std::endian endian = std::endian::native;

  using result_type = XXH128_hash_t;
  using seed_type = XXH64_hash_t;

  static result_type make(std::span<const std::byte> bytes) noexcept {
    TENZIR_ASSERT(bytes.data() != nullptr or bytes.empty());
    return XXH3_128bits(bytes.data(), bytes.size());
  }

  static result_type
  make(std::span<const std::byte> bytes, seed_type seed) noexcept {
    TENZIR_ASSERT(bytes.data() != nullptr or bytes.empty());
    return XXH3_128bits_withSeed(bytes.data(), bytes.size(), seed);
  }

  xxh3_128() noexcept : seed_{0} {
    XXH3_INITSTATE(&state_);
    reset();
  }

  explicit xxh3_128(seed_type seed) noexcept : seed_{seed} {
    std::memset(&state_, 0, sizeof(state_));
    reset();
  }

  void reset() noexcept {
    XXH3_128bits_reset_withSeed(&state_, seed_);
  }

  void add(std::span<const std::byte> bytes) noexcept {
    TENZIR_ASSERT(bytes.data() != nullptr or bytes.empty());
    XXH3_128bits_update(&state_, bytes.data(), bytes.size());
  }

  result_type finish() noexcept {
    return XXH3_128bits_digest(&state_);
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, xxh3_128& x) {
    return f(x.state_);
  }

private:
  XXH3_state_t state_;
  seed_type seed_ = 0;
};

} // namespace tenzir
