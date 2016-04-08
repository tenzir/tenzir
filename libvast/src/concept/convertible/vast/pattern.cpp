#include "vast/json.hpp"
#include "vast/pattern.hpp"
#include "vast/concept/convertible/vast/pattern.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/pattern.hpp"

namespace vast {

bool convert(pattern const& p, json& j) {
  j = to_string(p);
  return true;
}

} // namespace vast
