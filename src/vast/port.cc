#include "vast/port.h"

#include <utility>
#include "vast/logger.h"
#include "vast/serialization/arithmetic.h"
#include "vast/util/json.h"

namespace vast {

port::port(number_type number, port_type type)
  : number_(number)
  , type_(type)
{
}

port::number_type port::number() const
{
  return number_;
}

port::port_type port::type() const
{
  return type_;
}

void port::number(number_type n)
{
  number_ = n;
}

void port::type(port_type t)
{
  type_ = t;
}

void port::serialize(serializer& sink) const
{
  VAST_ENTER_WITH(VAST_THIS);
  typedef std::underlying_type<port::port_type>::type underlying_type;
  sink << number_;
  sink << static_cast<underlying_type>(type_);
}

void port::deserialize(deserializer& source)
{
  VAST_ENTER();
  source >> number_;
  std::underlying_type<port::port_type>::type type;
  source >> type;
  type_ = static_cast<port::port_type>(type);
  VAST_LEAVE(VAST_THIS);
}

bool operator==(port const& x, port const& y)
{
  return x.number_ == y.number_ && x.type_ == y.type_;
}

bool operator<(port const& x, port const& y)
{
  return std::tie(x.number_, x.type_) < std::tie(y.number_, y.type_);
}

trial<void> convert(port const& p, util::json& j)
{
  j = to_string(p);
  return nothing;
}

} // namespace vast
