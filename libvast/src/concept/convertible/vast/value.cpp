#include "vast/json.hpp"
#include "vast/value.hpp"
#include "vast/concept/convertible/vast/data.hpp"
#include "vast/concept/convertible/vast/type.hpp"
#include "vast/concept/convertible/vast/value.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/type.hpp"
#include "vast/concept/printable/vast/data.hpp"

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
