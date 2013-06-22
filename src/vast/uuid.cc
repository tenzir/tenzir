#include "vast/uuid.h"

#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "vast/logger.h"
#include "vast/to_string.h"
#include "vast/io/serialization.h"

namespace vast {

uuid uuid::random()
{
  uuid u;
  u.id_ = boost::uuids::random_generator()();
  return u;
}

uuid uuid::nil()
{
  uuid u;
  u.id_ = boost::uuids::nil_uuid();
  return u;
}

uuid::uuid(std::string const& str)
{
  boost::uuids::string_generator()(str).swap(id_);
}

uuid::const_iterator uuid::begin() const
{
  return id_.begin();
}

uuid::const_iterator uuid::end() const
{
  return id_.end();
}

size_t uuid::hash() const
{
  return boost::uuids::hash_value(id_);
}

void uuid::serialize(io::serializer& sink)
{
  VAST_ENTER(VAST_THIS);
  sink.write_raw(&id_, sizeof(id_));
}

void uuid::deserialize(io::deserializer& source)
{
  VAST_ENTER();
  source.read_raw(&id_, sizeof(id_));
  VAST_LEAVE(VAST_THIS);
}

bool operator==(uuid const& x, uuid const& y)
{
  return x.id_ == y.id_;
}

bool operator<(uuid const& x, uuid const& y)
{
  return x.id_ < y.id_;
}

void swap(uuid& x, uuid& y)
{
  x.id_.swap(y.id_);
}

std::string to_string(uuid const& id)
{
  return boost::uuids::to_string(id.id_);
}

std::ostream& operator<<(std::ostream& out, uuid const& u)
{
  out << to_string(u);
  return out;
}

} // namespace vast

size_t std::hash<vast::uuid>::operator()(vast::uuid const& u) const
{
  return u.hash();
}
