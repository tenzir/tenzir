#include "vast/json.hpp"
#include "vast/address.hpp"
#include "vast/concept/convertible/vast/address.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/address.hpp"

namespace vast {

bool convert(address const& a, json& j) {
  j = to_string(a);
  return true;
}

} // namespace vast
