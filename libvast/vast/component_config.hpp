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

#pragma once

#include "vast/concept/parseable/parse.hpp"

#include <caf/config_value.hpp>
#include <caf/settings.hpp>
#include <caf/sum_type.hpp>

namespace vast {

template <class V>
bool assign(V& v, const caf::settings& m, std::string_view key) {
  auto cv = caf::get_if(&m, key);
  if (!cv)
    return true;
  if constexpr (caf::detail::tl_contains<caf::config_value::variant_type::types,
                                         V>::value) {
    auto x = caf::get_if<V>(&*cv);
    if (!x)
      return false;
    v = *x;
    return true;
  } else {
    auto x = caf::get_if<std::string>(&*cv);
    if (!x)
      return false;
    auto f = x->begin();
    return parse(f, x->end(), v);
  }
}

} // namespace vast
