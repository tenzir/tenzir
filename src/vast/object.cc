#include "vast/object.h"
#include "vast/io/serialization.h"

namespace vast {

object::object(uuid id)
  : id_(id)
{
}

uuid const& object::id() const
{
  return id_;
}

void object::id(uuid id)
{
  id_ = std::move(id);
}

void object::id(std::string const& str)
{
  id(uuid(str));
}

void object::serialize(io::serializer& sink)
{
  sink << id_;
}

void object::deserialize(io::deserializer& source)
{
  source >> id_;
}

void swap(object& x, object& y)
{
  swap(x.id_, y.id_);
}

} // namespace vast
