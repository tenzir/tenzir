#include <tuple>

#include "vast/event.hpp"
#include "vast/json.hpp"

namespace vast {

event::event(none) {
}

event::event(value v) : value{std::move(v)} {
}

bool event::id(event_id i) {
  if (i <= max_event_id) {
    id_ = i;
    return true;
  }
  return false;
}

event_id event::id() const {
  return id_;
}

void event::timestamp(vast::timestamp ts) {
  timestamp_ = ts;
}

vast::timestamp event::timestamp() const {
  return timestamp_;
}

event flatten(event const& e) {
  event result = flatten(static_cast<value const&>(e));
  result.id(e.id());
  result.timestamp(e.timestamp());
  return result;
}

bool operator==(event const& x, event const& y) {
  return x.id() == y.id() &&
    x.timestamp() == y.timestamp() &&
    static_cast<value const&>(x) == static_cast<value const&>(y);
}

bool operator<(event const& x, event const& y) {
  return std::tie(x.id_, x.timestamp_, static_cast<value const&>(x)) <
    std::tie(y.id_, y.timestamp_, static_cast<value const&>(y));
}

bool convert(event const& e, json& j) {
  json::object o;
  o["id"] = e.id();
  o["timestamp"] = e.timestamp().time_since_epoch().count();
  if (!convert(static_cast<value const&>(e), o["value"]))
    return false;
  j = std::move(o);
  return true;
}
} // namespace vast
