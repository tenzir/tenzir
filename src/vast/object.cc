#include "vast/object.h"

#include <cassert>
#include "vast/serialization.h"

namespace vast {

object::object(void* value, global_type_info const* type)
  : value_(value), type_(type)
{
  assert(value_ != nullptr);
  assert(type_ != nullptr);
}

object::object(object const& other)
{
  if (other)
  {
    type_ = other.type_;
    value_ = type_->construct(other.value_);
  }
}

object::operator bool() const
{
  return value_ != nullptr && type_ != nullptr;
}

global_type_info const* object::type() const
{
  return type_;
}

void const* object::value() const
{
  return value_;
}

void* object::value()
{
  return value_;
}

void object::serialize(serializer& sink) const
{
  // TODO: relax these assertions and serialize some sort of "invalid object."
  assert(type_ != nullptr);
  assert(value_ != nullptr);

  sink.begin_object(*type_);
  type_->serialize(sink, value_);
  sink.end_object();
}

void object::deserialize(deserializer& source)
{
  if (value_ != nullptr)
  {
    assert(type_ != nullptr);
    type_->destroy(value_);
  }
  type_ = source.begin_object();
  if (type_ == nullptr)
    throw std::logic_error("invalid type info");
  value_ = type_->construct();
  type_->deserialize(source, value_);
  source.end_object();
}

} // namespace vast
