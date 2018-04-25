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

#include <sstream>

#include "vast/option_map.hpp"

#include "vast/concept/parseable/to.hpp"

#include "vast/detail/string.hpp"

namespace vast {


optional<data> option_map::get(const std::string& name) const {
  if (auto x = xs_.find(name); x != xs_.end())
    return x->second;
  return {};
}

data option_map::get_or(const std::string& name, const data& default_value) const {
  if(auto x = get(name); x)
    return *x;
  return default_value;
}

void option_map::set(const std::string& name, const data& x) {
  xs_[name] = x;
}


expected<void> option_map::add(const std::string& name, const data& x) {
  if (auto it = xs_.find(name); it != xs_.end()) 
    return make_error(ec::unspecified, "name: " + name + " already exist");
  set(name, x);
  return {};
}

} // namespace vast
