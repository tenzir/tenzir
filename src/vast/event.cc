#include "vast/event.h"

#include "vast/logger.h"
#include "vast/serialization.h"

namespace vast {

event::event(std::vector<value> values)
  : record(std::move(values))
{
}

event::event(std::initializer_list<value> args)
  : record(std::move(args))
{
}

event::event(event const& other)
  : record(other)
  , id_(other.id_)
  , timestamp_(other.timestamp_)
  , name_(other.name_)
{
}

event::event(event&& other)
  : record(std::move(other))
  , id_(std::move(other.id_))
  , timestamp_(std::move(other.timestamp_))
  , name_(std::move(other.name_))
{
}

event& event::operator=(event other)
{
  using std::swap;
  swap(*this, other);
  return *this;
}

uint64_t event::id() const
{
  return id_;
}

void event::id(uint64_t i)
{
  id_ = i;
}

string const& event::name() const
{
  return name_;
}

void event::name(string str)
{
  name_ = std::move(str);
}

time_point event::timestamp() const
{
  return timestamp_;
}

void event::timestamp(time_point time)
{
  timestamp_ = time;
}

void event::serialize(serializer& sink) const
{
  VAST_ENTER(VAST_THIS);
  sink << id_;
  sink << name_;
  sink << timestamp_;
  sink << static_cast<record const&>(*this);
}

void event::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> id_;
  source >> name_;
  source >> timestamp_;
  source >> static_cast<record&>(*this);
  VAST_LEAVE(VAST_THIS);
}

bool operator==(event const& x, event const& y)
{
  return
    x.id() == y.id() &&
    x.name_ == y.name_ &&
    x.timestamp_ == y.timestamp_ &&
    x.size() == y.size() &&
    std::equal(x.begin(), x.end(), y.begin());
}

bool operator<(event const& x, event const& y)
{
  return
    std::tie(x.name_, x.timestamp_, static_cast<record const&>(x)) <
    std::tie(y.name_, y.timestamp_, static_cast<record const&>(y));
}

void swap(event& x, event& y)
{
  using std::swap;
  swap(static_cast<record&>(x), static_cast<record&>(y));
  swap(x.id_, y.id_);
  swap(x.timestamp_, y.timestamp_);
  swap(x.name_, y.name_);
}

std::ostream& operator<<(std::ostream& out, event const& e)
{
  out 
    << (e.name().empty() ? "<unnamed>" : e.name())
    << " [" 
    << e.id() 
    << '|'
    << e.timestamp()
    << "] ";

  auto first = e.begin();
  auto last = e.end();
  while (first != last)
  {
    out << *first;
    if (++first != last)
      out << ", ";
  }

  return out;
}

} // namespace vast
