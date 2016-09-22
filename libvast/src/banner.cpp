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
