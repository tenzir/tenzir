#ifndef VAST_CONCEPT_SERIALIZABLE_UTIL_NONE_H
#define VAST_CONCEPT_SERIALIZABLE_UTIL_NONE_H

#include "vast/util/none.h"

namespace vast {

template <typename Serializer>
void serialize(Serializer&, util::none const&)
{
  // nop
}

template <typename Deserializer>
void deserialize(Deserializer&, util::none&)
{
  // nop
}

} // namespace vast

#endif
