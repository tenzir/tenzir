//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause
//
// This file comes from a 3rd party and has been adapted to fit into the VAST
// code base. Details about the original file:
//
// - Repository: https://github.com/pdillinger/wormhashing
// - Commit:     9d4e10bbae4c02dd4fbb03c84fb81388c62f74e7
// - Path:       bloom_simulation_tests
// - Author:     Peter Dillinger
// - Copyright:  (c) Peter C. Dillinger, (c) Facebook, Inc. and its affiliates.
// - License:    MIT

#include <cstddef>
#include <cstdint>

namespace vast::detail {

// TODO: maybe move to vast/config.hpp or alike?
#ifdef __SIZEOF_INT128__
constexpr bool have_128bit = true;
#else
constexpr bool have_128bit = false;
#endif

/// Wide odd regenerative multiplication (Worm). A generic version of fastrange
/// modulo reduction.
inline void wide_mul(size_t a, uint64_t h, size_t& upper, uint64_t& lower) {
  if constexpr (have_128bit) { // 64 bit
    __uint128_t wide = static_cast<__uint128_t>(a) * h;
    upper = static_cast<uint64_t>(wide >> 64);
    lower = static_cast<uint64_t>(wide);
  } else { // 32 bit
    uint64_t semiwide = (a & 0xffffffff) * (h & 0xffffffff);
    uint32_t lower_of_lower = static_cast<uint32_t>(semiwide);
    uint32_t upper_of_lower = static_cast<uint32_t>(semiwide >> 32);
    semiwide = (a & 0xffffffff) * (h >> 32);
    semiwide += upper_of_lower;
    upper = static_cast<size_t>(semiwide >> 32);
    lower = (semiwide << 32) | lower_of_lower;
  }
}

inline uint64_t fastrange64(uint64_t a, uint64_t h) {
  size_t result;
  uint64_t discard;
  wide_mul(a, h, result, discard);
  return result;
}

inline uint64_t worm64(size_t a, uint64_t& h) {
  size_t result;
  wide_mul(a, h, result, h);
  return result;
}

} // namespace vast::detail
