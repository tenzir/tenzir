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

#include "vast/timestamp_synopsis.hpp"

namespace vast {

timestamp_synopsis::timestamp_synopsis(vast::type x)
  : min_max_synopsis<timestamp>{std::move(x), timestamp::max(),
                                timestamp::min()} {
  // nop
}

bool timestamp_synopsis::equals(const synopsis& other) const noexcept {
  if (typeid(other) != typeid(timestamp_synopsis))
    return false;
  auto& dref = static_cast<const timestamp_synopsis&>(other);
  return type() == dref.type() && min() == dref.min() && max() == dref.max();
}

} // namespace vast
