#include "vast/json.h"
#include "vast/event.h"
#include "vast/concept/convertible/vast/event.h"
#include "vast/concept/convertible/vast/value.h"

namespace vast {

bool convert(event const& e, json& j)
{
  json::object o;
  o["id"] = e.id();
  o["timestamp"] = e.timestamp().time_since_epoch().count();
  if (! convert(static_cast<value const&>(e), o["value"]))
    return false;
  j = std::move(o);
  return true;
}

} // namespace vast
