#include "vast/json.h"
#include "vast/value.h"
#include "vast/concept/convertible/vast/data.h"
#include "vast/concept/convertible/vast/type.h"
#include "vast/concept/convertible/vast/value.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/type.h"
#include "vast/concept/printable/vast/data.h"

namespace vast {

bool convert(value const& v, json& j) {
  json::object o;
  if (!convert(v.type(), o["type"]))
    return false;
  if (!convert(v.data(), o["data"], v.type()))
    return false;
  j = std::move(o);
  return true;
}

} // namespace vast
