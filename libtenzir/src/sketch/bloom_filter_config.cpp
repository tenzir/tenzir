//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/sketch/bloom_filter_config.hpp"

#include <cmath>
#include <cstddef>

namespace tenzir::sketch {

namespace {

bloom_filter_params make(uint64_t m, uint64_t n, uint64_t k, double p) {
  // Make m odd for worm hashing to be regenerative.
  m -= ~(m & 1);
  return {m, n, k, p};
}

} // namespace

std::optional<bloom_filter_params> evaluate(bloom_filter_config cfg) {
  // Check basic invariants first.
  if (cfg.m and *cfg.m <= 0)
    return {};
  if (cfg.n and *cfg.n <= 0)
    return {};
  if (cfg.k and *cfg.k <= 0)
    return {};
  if (cfg.p and (*cfg.p < 0 or *cfg.p > 1))
    return {};
  // Test if we can compute the missing parameters.
  static const double ln2 = std::log(2.0);
  if (cfg.m and cfg.n and cfg.k and not cfg.p) {
    auto m = static_cast<double>(*cfg.m);
    auto n = static_cast<double>(*cfg.n);
    auto k = static_cast<double>(*cfg.k);
    auto r = m / n;
    auto q = std::exp(-k / r);
    auto p = std::pow(1 - q, k);
    return make(m, n, k, p);
  } else if (not cfg.m and cfg.n and not cfg.k and cfg.p) {
    auto n = static_cast<double>(*cfg.n);
    auto m = std::ceil(n * std::log(*cfg.p) / std::log(1 / std::exp2(ln2)));
    auto r = m / n;
    auto k = std::round(ln2 * r);
    auto q = std::exp(-k / r);
    auto p = std::pow(1 - q, k);
    return make(m, n, k, p);
  } else if (cfg.m and cfg.n and not cfg.k and not cfg.p) {
    auto m = static_cast<double>(*cfg.m);
    auto n = static_cast<double>(*cfg.n);
    auto r = m / n;
    auto k = std::round(ln2 * r);
    auto q = std::exp(-k / r);
    auto p = std::pow(1 - q, k);
    return make(m, n, k, p);
  } else if (cfg.m and not cfg.n and not cfg.k and cfg.p) {
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

} // namespace tenzir::sketch
