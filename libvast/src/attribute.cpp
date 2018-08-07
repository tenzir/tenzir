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

#include <tuple>

#include "vast/attribute.hpp"

namespace vast {

attribute::attribute(std::string key) : key{std::move(key)} {
}

attribute::attribute(std::string key, optional<std::string> value)
  : key{std::move(key)},
    value{std::move(value)} {
}

bool operator==(const attribute& x, const attribute& y) {
  return x.key == y.key && x.value == y.value;
}

bool operator<(const attribute& x, const attribute& y) {
  return std::tie(x.key, x.value) < std::tie(y.key, y.value);
}

} // namespace vast
