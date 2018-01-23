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

#include <algorithm>
#include <cctype>

#include "vast/http.hpp"

namespace vast {
namespace http {

header const* message::header(const std::string& name) const {
  auto pred = [&](auto& x) -> bool {
    if (x.name.size() != name.size())
      return false;
    for (auto i = 0u; i < name.size(); ++i)
      if (::toupper(x.name[i]) != ::toupper(name[i]))
        return false;
    return true;
  };
  auto i = std::find_if(headers.begin(), headers.end(), pred);
  return i == headers.end() ? nullptr : &*i;
}

} // namspace http
} // namspace vast
