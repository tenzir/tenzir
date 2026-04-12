//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/bloom_filter_parameters.hpp"

#include "tenzir/concept/parseable/core.hpp"
#include "tenzir/concept/parseable/numeric/integral.hpp"
#include "tenzir/concept/parseable/numeric/real.hpp"
#include "tenzir/detail/assert.hpp"

#include <cmath>
#include <cstddef>
#include <optional>
#include <string_view>

namespace tenzir {

std::optional<bloom_filter_parameters> evaluate(bloom_filter_parameters xs) {
  // Check basic invariants first.
  if (xs.m and *xs.m <= 0) {
    return {};
  }
  if (xs.n and *xs.n <= 0) {
    return {};
  }
  if (xs.k and *xs.k <= 0) {
    return {};
  }
  if (xs.p and (*xs.p < 0 or *xs.p > 1)) {
    return {};
  }
  // Test if we can compute the missing parameters.
  static const double ln2 = std::log(2.0);
  if (xs.m and xs.n and xs.k and not xs.p) {
    auto m = static_cast<double>(*xs.m);
    auto n = static_cast<double>(*xs.n);
    auto k = static_cast<double>(*xs.k);
    auto r = m / n;
    auto q = std::exp(-k / r);
    xs.p = std::pow(1 - q, k);
    return xs;
  } else if (not xs.m and xs.n and not xs.k and xs.p) {
    auto n = static_cast<double>(*xs.n);
    auto m = std::ceil(n * std::log(*xs.p) / std::log(1 / std::exp2(ln2)));
    auto r = m / n;
    auto k = std::round(ln2 * r);
    auto q = std::exp(-k / r);
    xs.m = m;
    xs.k = k;
    xs.p = std::pow(1 - q, k);
    return xs;
  } else if (xs.m and xs.n and not xs.k and not xs.p) {
    auto m = static_cast<double>(*xs.m);
    auto n = static_cast<double>(*xs.n);
    auto r = m / n;
    auto k = std::round(ln2 * r);
    auto q = std::exp(-k / r);
    xs.k = std::round(ln2 * r);
    xs.p = std::pow(1 - q, k);
    return xs;
  } else if (xs.m and not xs.n and not xs.k and xs.p) {
    auto m = static_cast<double>(*xs.m);
    auto n = std::ceil(m * std::log(1.0 / std::exp2(ln2)) / std::log(*xs.p));
    auto r = m / n;
    auto k = std::round(ln2 * r);
    auto q = std::exp(-k / r);
    xs.n = n;
    xs.k = k;
    xs.p = std::pow(1 - q, k);
    return xs;
  }
  return {};
}

std::optional<bloom_filter_parameters> parse_parameters(std::string_view x) {
  using namespace parser_literals;
  using parsers::real;
  using parsers::u64;
  auto parser = "bloomfilter("_p >> u64 >> ',' >> real >> ')';
  bloom_filter_parameters xs;
  xs.n = 0;
  xs.p = 0;
  if (parser(x, *xs.n, *xs.p)) {
    return xs;
  }
  return {};
}

bool operator==(const bloom_filter_parameters& x,
                const bloom_filter_parameters& y) {
  return x.m == y.m and x.n == y.n and x.k == y.k and x.p == y.p;
}

bool operator!=(const bloom_filter_parameters& x,
                const bloom_filter_parameters& y) {
  return not(x == y);
}

} // namespace tenzir
