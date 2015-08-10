#include <sstream>

#include "vast/config.h"
#include "vast/banner.h"
#include "vast/util/color.h"

namespace vast {

std::string banner(bool colorize) {
  std::stringstream ss;
  if (colorize)
    ss << util::color::red;
  ss << "     _   _____   __________\n"
        "    | | / / _ | / __/_  __/\n"
        "    | |/ / __ |_\\ \\  / /\n"
        "    |___/_/ |_/___/ /_/  ";
  if (colorize)
    ss << util::color::yellow;
  ss << VAST_VERSION;
  if (colorize)
    ss << util::color::reset;
  return ss.str();
}

} // namespace vast
