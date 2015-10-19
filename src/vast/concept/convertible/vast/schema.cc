#include "vast/json.h"
#include "vast/schema.h"
#include "vast/concept/convertible/vast/schema.h"
#include "vast/concept/convertible/vast/type.h"
#include "vast/concept/printable/to_string.h"
#include "vast/concept/printable/vast/address.h"
#include "vast/concept/printable/vast/type.h"

namespace vast {

bool convert(schema const& s, json& j) {
  json::object o;
  json::array a;
  std::transform(s.begin(), s.end(), std::back_inserter(a),
                 [](auto& t) { return to_json(t); });
  o["types"] = std::move(a);
  j = std::move(o);
  return true;
}

} // namespace vast
