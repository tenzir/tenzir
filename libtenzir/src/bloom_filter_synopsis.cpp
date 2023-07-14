//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/bloom_filter_synopsis.hpp"

#include "tenzir/detail/assert.hpp"

namespace tenzir {

type annotate_parameters(const type& x, const bloom_filter_parameters& params) {
  auto v = fmt::format("bloomfilter({},{})", *params.n, *params.p);
  return type{x, {{"synopsis", std::move(v)}}};
}

std::optional<bloom_filter_parameters> parse_parameters(const type& x) {
  auto synopsis = x.attribute("synopsis");
  if (!synopsis || synopsis->empty())
    return {};
  return parse_parameters(*synopsis);
}

} // namespace tenzir
