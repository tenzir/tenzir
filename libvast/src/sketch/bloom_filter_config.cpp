//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/sketch/bloom_filter_config.hpp"

#include <cmath>
#include <cstddef>

namespace vast::sketch {

namespace {

bloom_filter_params make(uint64_t m, uint64_t n, uint64_t k, double p) {
  // Make m odd for worm hashing to be regenerative.
  m -= ~(m & 1);
  return {m, n, k, p};
}

} // namespace

std::optional<bloom_filter_params> evaluate(bloom_filter_config cfg) {
  // Check basic invariants first.
  if (cfg.m && *cfg.m <= 0)
    return {};
  if (cfg.n && *cfg.n <= 0)
    return {};
  if (cfg.k && *cfg.k <= 0)
    return {};
  if (cfg.p && (*cfg.p < 0 || *cfg.p > 1))
    return {};
  // Test if we can compute the missing parameters.
  static const double ln2 = std::log(2.0);
  if (cfg.m && cfg.n && cfg.k && !cfg.p) {
    auto m = static_cast<double>(*cfg.m);
    auto n = static_cast<double>(*cfg.n);
    auto k = static_cast<double>(*cfg.k);
    auto r = m / n;
    auto q = std::exp(-k / r);
    auto p = std::pow(1 - q, k);
    return make(m, n, k, p);
  } else if (!cfg.m && cfg.n && !cfg.k && cfg.p) {
    auto n = static_cast<double>(*cfg.n);
    auto m = std::ceil(n * std::log(*cfg.p) / std::log(1 / std::exp2(ln2)));
    auto r = m / n;
    auto k = std::round(ln2 * r);
    auto q = std::exp(-k / r);
    auto p = std::pow(1 - q, k);
    return make(m, n, k, p);
  } else if (cfg.m && cfg.n && !cfg.k && !cfg.p) {
    auto m = static_cast<double>(*cfg.m);
    auto n = static_cast<double>(*cfg.n);
    auto r = m / n;
    auto k = std::round(ln2 * r);
    auto q = std::exp(-k / r);
    auto p = std::pow(1 - q, k);
    return make(m, n, k, p);
  } else if (cfg.m && !cfg.n && !cfg.k && cfg.p) {
    auto m = static_cast<double>(*cfg.m);
    auto n = std::ceil(m * std::log(1.0 / std::exp2(ln2)) / std::log(*cfg.p));
    auto r = m / n;
    auto k = std::round(ln2 * r);
    auto q = std::exp(-k / r);
    auto p = std::pow(1 - q, k);
    return make(m, n, k, p);
  }
  return {};
}

} // namespace vast::sketch
