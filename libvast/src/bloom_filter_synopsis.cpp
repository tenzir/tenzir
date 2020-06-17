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

namespace vast {

caf::optional<bloom_filter_parameters> parse_parameters(const type& x) {
  auto pred = [](auto& attr) {
    return attr.key == "synopsis" && attr.value != caf::none;
  };
  auto i = std::find_if(x.attributes().begin(), x.attributes().end(), pred);
  if (i != x.attributes().end())
    return parse_parameters(*i->value);
  return caf::none;
}

} // namespace vast
