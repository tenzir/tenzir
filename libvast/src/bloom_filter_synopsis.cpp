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

#include "vast/bloom_filter_synopsis.hpp"

#include <vast/detail/assert.hpp>

namespace vast {

type annotate_parameters(type type, const bloom_filter_parameters& params) {
  using namespace std::string_literals;
  auto v = "bloomfilter("s + std::to_string(*params.n) + ','
           + std::to_string(*params.p) + ')';
  // Replaces any previously existing attributes.
  return std::move(type).attributes({{"synopsis", std::move(v)}});
}

caf::optional<bloom_filter_parameters> parse_parameters(const type& x) {
  auto pred = [](auto& attr) {
    return attr.key == "synopsis" && attr.value != caf::none;
  };
  auto i = std::find_if(x.attributes().begin(), x.attributes().end(), pred);
  if (i == x.attributes().end())
    return caf::none;
  VAST_ASSERT(i->value);
  return parse_parameters(*i->value);
}

} // namespace vast
