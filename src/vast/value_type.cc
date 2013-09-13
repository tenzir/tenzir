#include "vast/value_type.h"

#include "vast/logger.h"
#include "vast/serialization.h"
#include "vast/util/print.h"

namespace vast {

std::ostream& operator<<(std::ostream& out, value_type t)
{
  stream_to(out, t);
  return out;
}

void serialize(serializer& sink, value_type x)
{
  VAST_ENTER("value_type: " << VAST_ARG(x));
  sink << static_cast<std::underlying_type<value_type>::type>(x);
}

void deserialize(deserializer& source, value_type& x)
{
  VAST_ENTER();
  std::underlying_type<value_type>::type u;
  source >> u;
  x = static_cast<value_type>(u);
  VAST_LEAVE("value_type: " << x);
}

} // namespace vast
