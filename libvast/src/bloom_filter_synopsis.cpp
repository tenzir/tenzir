//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/bloom_filter_synopsis.hpp"

#include "vast/detail/assert.hpp"

namespace vast {

legacy_type
annotate_parameters(legacy_type type, const bloom_filter_parameters& params) {
  using namespace std::string_literals;
  auto v = "bloomfilter("s + std::to_string(*params.n) + ','
           + std::to_string(*params.p) + ')';
  // Replaces any previously existing attributes.
  return std::move(type).attributes({{"synopsis", std::move(v)}});
}

std::optional<bloom_filter_parameters> parse_parameters(const legacy_type& x) {
  auto pred = [](auto& attr) {
    return attr.key == "synopsis" && attr.value != caf::none;
  };
  auto i = std::find_if(x.attributes().begin(), x.attributes().end(), pred);
  if (i == x.attributes().end())
    return {};
  VAST_ASSERT(i->value);
  return parse_parameters(*i->value);
}

} // namespace vast
