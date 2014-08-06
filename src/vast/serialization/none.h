#ifndef VAST_SERIALIZATION_NONE_H
#define VAST_SERIALIZATION_NONE_H

#include "vast/serialization/arithmetic.h"
#include "vast/util/none.h"

namespace vast {

inline void serialize(serializer& sink, util::none const&)
{
  sink << true;
}

inline void deserialize(deserializer& source, util::none&)
{
  bool flag;
  source >> flag;
}

} // namespace vast

#endif
