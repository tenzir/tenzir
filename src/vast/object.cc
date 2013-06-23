#include "vast/object.h"

#include <cassert>
#include "vast/serialization.h"

namespace vast {

object::object(void* value, global_type_info const* type)
  : value_(value), type_(type)
{
  assert(value);
  assert(type);
}

object::object(object const& other)
{
  if (other)
  {
    type_ = other.type_;
    value_ = type_->create(other.value_);
  }
}

object::operator bool() const
{
  return value_ != nullptr;
}

global_type_info const* object::type() const
{
  return type_;
}

void const* object::value() const
{
  return value_;
}

void* object::mutable_value() const
{
  return value_;
}


} // namespace vast
