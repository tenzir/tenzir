#include "vast/event.h"

namespace vast {

event::event(none) {
}

event::event(value v) : value{std::move(v)} {
}

bool operator==(event const& x, event const& y) {
  return x.id() == y.id() && x.timestamp() == y.timestamp()
         && static_cast<value const&>(x) == static_cast<value const&>(y);
}

bool operator<(event const& x, event const& y) {
  return std::tie(x.id_, x.timestamp_, static_cast<value const&>(x))
         < std::tie(y.id_, y.timestamp_, static_cast<value const&>(y));
}

bool event::id(event_id i) {
  if (i <= max_event_id) {
    id_ = i;
    return true;
  }
  return false;
}

void event::timestamp(time::point time) {
  timestamp_ = time;
}

event_id event::id() const {
  return id_;
}

time::point event::timestamp() const {
  return timestamp_;
}

event flatten(event const& e) {
  event result = flatten(static_cast<value const&>(e));
  result.id(e.id());
  result.timestamp(e.timestamp());
  return result;
}

} // namespace vast
