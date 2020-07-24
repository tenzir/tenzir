
#include "vast/status.hpp"

namespace vast {

caf::dictionary<caf::config_value> join(const status& s) {
  caf::dictionary<caf::config_value> result;
  result["info"] = s.info;
  result["verbose"] = s.verbose;
  result["debug"] = s.debug;
  return result;
}

} // namespace vast
