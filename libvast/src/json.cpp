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

#include "vast/json.hpp"

#include <type_traits>

#include <caf/atom.hpp>
#include <caf/config_value.hpp>

#include "vast/detail/overload.hpp"
#include "vast/detail/type_traits.hpp"

namespace vast {

bool convert(const caf::config_value& x, json& j) {
  using detail::is_any_v;
  using std::is_same_v;
  using namespace caf;
  return visit(
    [&](const auto& y) {
      using type = std::decay_t<decltype(y)>;
      if constexpr (is_any_v<type, atom_value, timespan, timestamp, uri>)
        return convert(deep_to_string(y), j);
      else
        return convert(y, j);
    }, x);
}

} // namespace vast
