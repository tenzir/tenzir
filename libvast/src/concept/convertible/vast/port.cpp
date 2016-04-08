#include "vast/json.hpp"
#include "vast/port.hpp"
#include "vast/concept/convertible/vast/port.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/port.hpp"

namespace vast {

bool convert(port const& p, json& j) {
  j = to_string(p);
  return true;
}

} // namespace vast
