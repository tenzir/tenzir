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
#include <utility>

namespace vast::detail {

inline std::pair<uint64_t, uint64_t> wide_mul(uint64_t a, uint64_t h) {
  __uint128_t wide = static_cast<__uint128_t>(a) * h;
  return {static_cast<uint64_t>(wide >> 64), static_cast<uint64_t>(wide)};
}

/// Lemire's fast modulo reduction.
inline uint64_t fastrange64(uint64_t a, uint64_t h) {
  return wide_mul(a, h).first;
}

/// Dillinger's wide odd regenerative multiplication.
inline uint64_t worm64(uint64_t a, uint64_t& h) {
  auto result = wide_mul(a, h);
  h = result.second;
  return result.first;
}

} // namespace vast::detail
