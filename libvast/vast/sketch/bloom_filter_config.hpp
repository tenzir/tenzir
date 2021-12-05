//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <cstdint>
#include <optional>

namespace vast::sketch {

/// The parameters to construct a Bloom filter. Only a subset of parameter
/// combinations is viable in practice. One of the following 4 combinations can
/// determine all other paramters:
///
/// 1. *(m, n, k)*
/// 2. *(n, p)*
/// 3. *(m, n)*
/// 4. *(m, p)*
///
struct bloom_filter_config {
  std::optional<uint64_t> m; ///< Number of cells/bits.
  std::optional<uint64_t> n; ///< Set cardinality.
  std::optional<uint64_t> k; ///< Number of hash functions.
  std::optional<double> p;   ///< False-positive probability.
};

/// A set of evaluated Bloom filter parameters. Typically, this is the result
/// of an evaluated Bloom filter configuration. The following invariants must
/// hold at all times:
///
/// - m > 0
/// - n > 0
/// - k > 0
/// - 0.0 < p < 1.0
///
/// Otherwise we do not have a valid parameterization.
struct bloom_filter_params {
  uint64_t m; ///< Number of cells/bits.
  uint64_t n; ///< Set cardinality.
  uint64_t k; ///< Number of hash functions.
  double p;   ///< False-positive probability.

  friend bool
  operator<=>(const bloom_filter_params& x, const bloom_filter_params& y)
    = default;
};

/// Evaluates a set of Bloom filter parameters. Some parameters can derived
/// from a specific combination of others. If the correct parameters are
/// provided, this function computes the remaining ones.
///
/// If m is given and even, the evaluation subtracts 1 to make m odd. This
/// ensures that the parameterization can be used for filters that use worm
/// hashing. (If m was even, each multiplication would stack zeros in the
/// lowest bits and prevent worm hashing from being regenerative.) This
/// "off-by-one" effect has neglible impact in nearly all applictions, except
/// for incredibly small Bloom filters.
///
/// @returns The complete set of parameters
std::optional<bloom_filter_params> evaluate(bloom_filter_config cfg);

} // namespace vast::sketch
