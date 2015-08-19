#include "vast/json.h"
#include "vast/address.h"
#include "vast/concept/convertible/vast/address.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/address.h"

namespace vast {

bool convert(address const& a, json& j) {
  j = to_string(a);
  return true;
}

} // namespace vast
