#include "vast/json.h"
#include "vast/pattern.h"
#include "vast/concept/convertible/vast/pattern.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/pattern.h"

namespace vast {

bool convert(pattern const& p, json& j) {
  j = to_string(p);
  return true;
}

} // namespace vast
