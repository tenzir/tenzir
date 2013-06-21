#include "vast/individual.h"
#include "vast/io/serialization.h"

namespace vast {

individual::individual(uuid id)
  : id_(id)
{
}

uuid const& individual::id() const
{
  return id_;
}

void individual::id(uuid id)
{
  id_ = std::move(id);
}

void individual::serialize(io::serializer& sink)
{
  sink << id_;
}

void individual::deserialize(io::deserializer& source)
{
  source >> id_;
}

void swap(individual& x, individual& y)
{
  swap(x.id_, y.id_);
}

} // namespace vast
