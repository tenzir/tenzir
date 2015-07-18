#include "vast/json.h"
#include "vast/port.h"
#include "vast/concept/convertible/vast/port.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/port.h"

namespace vast {

bool convert(port const& p, json& j)
{
  j = to_string(p);
  return true;
}

} // namespace vast
