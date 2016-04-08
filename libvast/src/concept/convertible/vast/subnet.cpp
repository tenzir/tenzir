#include "vast/json.hpp"
#include "vast/subnet.hpp"
#include "vast/concept/convertible/vast/subnet.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/subnet.hpp"

namespace vast {

bool convert(subnet const& sn, json& j) {
  j = to_string(sn);
  return true;
}

} // namespace vast
