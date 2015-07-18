#include <tuple>

#include "vast/port.h"

namespace vast {

port::port(number_type number, port_type type)
  : number_(number)
  , type_(type)
{
}

bool operator==(port const& x, port const& y)
{
  return x.number_ == y.number_ && x.type_ == y.type_;
}

bool operator<(port const& x, port const& y)
{
  return std::tie(x.number_, x.type_) < std::tie(y.number_, y.type_);
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

} // namespace vast
