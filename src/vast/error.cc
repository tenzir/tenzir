#include "vast/error.h"

#include "vast/serialization/string.h"

namespace vast {

void serialize(serializer& sink, error const& e)
{
  sink << e.msg();
}

void deserialize(deserializer& source, error& e)
{
  std::string str;
  source >> str;
  e = error{std::move(str)};
}

} // namespace vast
