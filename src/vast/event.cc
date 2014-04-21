#include "vast/event.h"

#include "vast/logger.h"
#include "vast/serialization.h"

namespace vast {

event::event()
  : type_{type_invalid}
{
}

event::event(record values)
  : record(std::move(values)),
    type_{type_invalid}
{
}

event::event(std::initializer_list<value> list)
  : record(std::move(list)),
    type_{type_invalid}
{
}

void event::id(event_id i)
{
  id_ = i;
}

void event::timestamp(time_point time)
{
  timestamp_ = time;
}

void event::type(type_const_ptr t)
{
  assert(t);
  type_ = std::move(t);
}

event_id event::id() const
{
  return id_;
}

time_point event::timestamp() const
{
  return timestamp_;
}

type_const_ptr const& event::type() const
{
  return type_;
}

string const& event::name() const
{
  return type_->name();
}

void event::serialize(serializer& sink) const
{
  assert(type_);
  sink
    << *type_
    << id_
    << timestamp_
    << static_cast<record const&>(*this);
}

void event::deserialize(deserializer& source)
{
  auto t = type::make<invalid_type>();

  source
    >> *t
    >> id_
    >> timestamp_
    >> static_cast<record&>(*this);

  type_ = t;
}

bool operator==(event const& x, event const& y)
{
  return x.id() == y.id()
      && ((x.type_ && y.type_ && *x.type_ == *y.type_)
          || (! x.type_ && ! y.type_))
      && x.timestamp_ == y.timestamp_
      && x.size() == y.size()
      && std::equal(x.begin(), x.end(), y.begin());
}

bool operator<(event const& x, event const& y)
{
  if (x.type_ && y.type_)
    return
      std::tie(x.id_, x.timestamp_, *x.type_, static_cast<record const&>(x)) <
      std::tie(y.id_, y.timestamp_, *y.type_, static_cast<record const&>(y));
  else
    return
      std::tie(x.id_, x.timestamp_, static_cast<record const&>(x)) <
      std::tie(y.id_, y.timestamp_, static_cast<record const&>(y));
}

} // namespace vast
