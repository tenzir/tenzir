#include <cstdlib>
#include <iostream>

#include "vast/die.h"

namespace vast {

[[noreturn]] void die(std::string const& msg)
{
  if (! msg.empty())
    std::cerr << "\nERROR: " << msg << std::endl;
  std::abort();
}

} // namespace vast
