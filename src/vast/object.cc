#include "vast/object.h"

#include <cassert>
#include "vast/serialization.h"

namespace vast {

object::object(global_type_info const* type, void* value)
  : type_(type), value_(value)
{
  assert(type_ != nullptr);
  assert(value_ != nullptr);
}

object::object(object const& other)
{
  if (other)
  {
    type_ = other.type_;
    value_ = type_->construct(other.value_);
  }
}

object::object(object&& other)
  : type_(other.type_), value_(other.value_)
{
  other.type_ = nullptr;
  other.value_ = nullptr;
}

object& object::operator=(object other)
{
  std::swap(type_, other.type_);
  std::swap(value_, other.value_);
  return *this;
}

object::~object()
{
  if (*this)
    type_->destruct(value_);
}

object::operator bool() const
{
  return value_ != nullptr && type_ != nullptr;
}

bool operator==(object const& x, object const& y)
{
  return x.type() == y.type()
    ? (x.value() == y.value() || x.type()->equals(x.value(), y.value()))
    : false;
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

void* object::release()
{
  auto ptr = value_;
  type_ = nullptr;
  value_ = nullptr;
  return ptr;
}

void object::serialize(serializer& sink) const
{
  VAST_ENTER();
  assert(*this);
  sink.write_type(type_);
  type_->serialize(sink, value_);
}

void object::deserialize(deserializer& source)
{
  VAST_ENTER();
  if (*this)
    type_->destruct(value_);
  if (! source.read_type(type_) )
  {
    VAST_LOG_ERROR("failed to deserialize object type");
  }
  else if (type_ == nullptr)
  {
    VAST_LOG_ERROR("deserialized an invalid object type");
  }
  else
  {
    value_ = type_->construct();
    type_->deserialize(source, value_);
  }
}

} // namespace vast
