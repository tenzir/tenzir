//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/bloom_filter_parameters.hpp"

#include "vast/concept/parseable/core.hpp"
#include "vast/concept/parseable/numeric/integral.hpp"
#include "vast/concept/parseable/numeric/real.hpp"
#include "vast/detail/assert.hpp"

#include <cmath>
#include <cstddef>
#include <optional>

namespace vast {

std::optional<bloom_filter_parameters> evaluate(bloom_filter_parameters xs) {
  // Check basic invariants first.
  if (xs.m && *xs.m <= 0)
    return {};
  if (xs.n && *xs.n <= 0)
    return {};
  if (xs.k && *xs.k <= 0)
    return {};
  if (xs.p && (*xs.p < 0 || *xs.p > 1))
    return {};
  // Test if we can compute the missing parameters.
  static const double ln2 = std::log(2.0);
  if (xs.m && xs.n && xs.k && !xs.p) {
    auto m = static_cast<double>(*xs.m);
    auto n = static_cast<double>(*xs.n);
    auto k = static_cast<double>(*xs.k);
    auto r = m / n;
    auto q = std::exp(-k / r);
    xs.p = std::pow(1 - q, k);
    return xs;
  } else if (!xs.m && xs.n && !xs.k && xs.p) {
    auto n = static_cast<double>(*xs.n);
    auto m = std::ceil(n * std::log(*xs.p) / std::log(1 / std::exp2(ln2)));
    auto r = m / n;
    auto k = std::round(ln2 * r);
    auto q = std::exp(-k / r);
    xs.m = m;
    xs.k = k;
    xs.p = std::pow(1 - q, k);
    return xs;
  } else if (xs.m && xs.n && !xs.k && !xs.p) {
    auto m = static_cast<double>(*xs.m);
    auto n = static_cast<double>(*xs.n);
    auto r = m / n;
    auto k = std::round(ln2 * r);
    auto q = std::exp(-k / r);
    xs.k = std::round(ln2 * r);
    xs.p = std::pow(1 - q, k);
    return xs;
  } else if (xs.m && !xs.n && !xs.k && xs.p) {
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
  using parsers::real_opt_dot;
  using parsers::u64;
  auto parser = "bloomfilter("_p >> u64 >> ',' >> real_opt_dot >> ')';
  bloom_filter_parameters xs;
  xs.n = 0;
  xs.p = 0;
  if (parser(x, *xs.n, *xs.p))
    return xs;
  return {};
}

bool operator==(const bloom_filter_parameters& x,
                const bloom_filter_parameters& y) {
  return x.m == y.m && x.n == y.n && x.k == y.k && x.p == y.p;
}

bool operator!=(const bloom_filter_parameters& x,
                const bloom_filter_parameters& y) {
  return !(x == y);
}

} // namespace vast
