#include "vast/type_tag.h"

#include "vast/logger.h"
#include "vast/serialization.h"

namespace vast {

void serialize(serializer& sink, type_tag x) {
  VAST_ENTER("type tag: " << VAST_ARG(x));
  sink << static_cast<std::underlying_type<type_tag>::type>(x);
}

void deserialize(deserializer& source, type_tag& x) {
  VAST_ENTER();
  std::underlying_type<type_tag>::type u;
  source >> u;
  x = static_cast<type_tag>(u);
  VAST_LEAVE("type tag: " << x);
}

} // namespace vast
