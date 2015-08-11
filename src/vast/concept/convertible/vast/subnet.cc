#include "vast/json.h"
#include "vast/subnet.h"
#include "vast/concept/convertible/vast/subnet.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/subnet.h"

namespace vast {

bool convert(subnet const& sn, json& j) {
  j = to_string(sn);
  return true;
}

} // namespace vast
