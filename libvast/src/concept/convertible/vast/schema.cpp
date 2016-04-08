#include "vast/json.hpp"
#include "vast/schema.hpp"
#include "vast/concept/convertible/vast/schema.hpp"
#include "vast/concept/convertible/vast/type.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/address.hpp"
#include "vast/concept/printable/vast/type.hpp"

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
