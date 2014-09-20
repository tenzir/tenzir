#include "vast/event.h"

#include "vast/logger.h"
#include "vast/serialization/arithmetic.h"
#include "vast/util/json.h"

namespace vast {

event::event(none)
{
}

event::event(value v)
  : value{std::move(v)}
{
}

bool event::id(event_id i)
{
  if (i <= max_event_id)
  {
    id_ = i;
    return true;
  }

  return false;
}

void event::timestamp(time_point time)
{
  timestamp_ = time;
}

event_id event::id() const
{
  return id_;
}

time_point event::timestamp() const
{
  return timestamp_;
}

void event::serialize(serializer& sink) const
{
  sink
    << id_
    << timestamp_
    << static_cast<value const&>(*this);
}

void event::deserialize(deserializer& source)
{
  source
    >> id_
    >> timestamp_
    >> static_cast<value&>(*this);
}

bool operator==(event const& x, event const& y)
{
  return x.id() == y.id()
      && x.timestamp() == y.timestamp()
      && static_cast<value const&>(x) == static_cast<value const&>(y);
}

bool operator<(event const& x, event const& y)
{
  return
    std::tie(x.id_, x.timestamp_, static_cast<value const&>(x)) <
    std::tie(y.id_, y.timestamp_, static_cast<value const&>(y));
}

trial<void> convert(event const& e, util::json& j)
{
  util::json::object o;
  o["id"] = e.id();

  auto t = to<util::json>(e.timestamp().since_epoch().count());
  if (! t)
    return t.error();
  o["timestamp"] = *t;

  t = to<util::json>(static_cast<value const&>(e));
  if (! t)
    return t.error();
  o["value"] = *t;

  j = std::move(o);
  return nothing;
}

} // namespace vast
