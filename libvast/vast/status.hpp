
#pragma once

#include <caf/config_value.hpp>
#include <caf/dictionary.hpp>

namespace vast {

struct status {
  caf::dictionary<caf::config_value> info;
  caf::dictionary<caf::config_value> verbose;
  caf::dictionary<caf::config_value> debug;
};

caf::dictionary<caf::config_value> join(const status& s);

} // namespace vast
