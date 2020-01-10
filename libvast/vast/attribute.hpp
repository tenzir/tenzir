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

#include <string>

#include "vast/detail/operators.hpp"
#include "vast/optional.hpp"

namespace vast {

/// A qualifier in the form of a key and optional value.
struct attribute : detail::totally_ordered<attribute> {
  attribute(std::string key = {});
  attribute(std::string key, optional<std::string> value);

  std::string key;
  optional<std::string> value;
};

/// @relates attribute
bool operator==(const attribute& x, const attribute& y);

/// @relates attribute
bool operator<(const attribute& x, const attribute& y);

/// @relates attribute
template <class Inspector>
auto inspect(Inspector& f, attribute& a) {
  return f(a.key, a.value);
}

} // namespace vast
