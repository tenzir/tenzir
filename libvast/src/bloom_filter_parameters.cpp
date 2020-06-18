/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/bloom_filter_parameters.hpp"

#include <cmath>
#include <cstddef>

#include <vast/concept/parseable/core.hpp>
#include <vast/concept/parseable/numeric/integral.hpp>
#include <vast/concept/parseable/numeric/real.hpp>
#include <vast/detail/assert.hpp>
#include <vast/type.hpp>

namespace vast {

caf::optional<bloom_filter_parameters> evaluate(bloom_filter_parameters xs) {
  VAST_ASSERT(!xs.m || *xs.m > 0);
  VAST_ASSERT(!xs.n || *xs.n > 0);
  VAST_ASSERT(!xs.k || *xs.k > 0);
  VAST_ASSERT(!xs.p || (*xs.p > 0 && *xs.p < 1));
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
  return caf::none;
}

caf::optional<bloom_filter_parameters> parse_parameters(std::string_view x) {
  using namespace parser_literals;
  using parsers::real_opt_dot;
  using parsers::u64;
  auto parser = "bloomfilter("_p >> u64 >> ',' >> real_opt_dot >> ')';
  bloom_filter_parameters xs;
  xs.n = 0;
  xs.p = 0;
  if (parser(x, *xs.n, *xs.p))
    return xs;
  return caf::none;
}

} // namespace vast
