#include "vast/io/compression.h"

#include "vast/io/serialization.h"

namespace vast {
namespace io {

void serialize(serializer& sink, compression method)
{
  sink << static_cast<std::underlying_type<compression>::type>(method);
}

void deserialize(deserializer& source, compression& method)
{
  std::underlying_type<compression>::type u;
  source >> u;
  method = static_cast<compression>(u);
}

} // namespace io
} // namespace vast
