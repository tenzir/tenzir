//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/fwd.hpp"

#include <optional>
#include <string_view>

namespace vast {

/// The parameters to construct a Bloom filter. Only a subset of parameter
/// combinations is viable in practice. One of the following 4 combinations can
/// determine all other paramters:
///
/// 1. *(m, n, k)*
/// 2. *(n, p)*
/// 3. *(m, n)*
/// 4. *(m, p)*
///
/// The following invariants must hold at all times:
///
/// - `!m || *m > 0`
/// - `!n || *n > 0`
/// - `!k || *k > 0`
/// - `!p || 0.0 < p < 1.0`
///
/// @relates bloom_filter
struct bloom_filter_parameters {
  std::optional<size_t> m; ///< Number of cells/bits.
  std::optional<size_t> n; ///< Set cardinality.
  std::optional<size_t> k; ///< Number of hash functions.
  std::optional<double> p; ///< False-positive probability.
};

/// Evaluates a set of Bloom filter parameters. Some parameters can derived
/// from a specific combination of others. If the correct parameters are
/// provided, this function computes the remaining ones.
/// @returns The complete set of parameters
std::optional<bloom_filter_parameters> evaluate(bloom_filter_parameters xs);

/// Attempts to Bloom filter parameters of the form `bloom_filter(n,p)`, where
/// `n` and `p` are floating-point values.
/// @param x The input to parse.
/// @returns The parsed Bloom filter parameters.
/// @relates evaluate
std::optional<bloom_filter_parameters> parse_parameters(std::string_view x);

} // namespace vast
