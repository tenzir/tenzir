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

#include "vast/config.hpp"
#include "vast/banner.hpp"
#include "vast/detail/color.hpp"

namespace vast {

std::string banner(bool colorize) {
  std::stringstream ss;
  if (colorize)
    ss << detail::color::red;
  ss << "     _   _____   __________\n"
        "    | | / / _ | / __/_  __/\n"
        "    | |/ / __ |_\\ \\  / /\n"
        "    |___/_/ |_/___/ /_/  ";
  if (colorize)
    ss << detail::color::yellow;
  ss << VAST_VERSION;
  if (colorize)
    ss << detail::color::reset;
  return ss.str();
}

} // namespace vast
