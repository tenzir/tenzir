#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_NONE_H
#define VAST_CONCEPT_SERIALIZABLE_VAST_NONE_H

#include "vast/none.h"

namespace vast {

template <typename Serializer>
void serialize(Serializer&, none const&) {
  // nop
}

template <typename Deserializer>
void deserialize(Deserializer&, none&) {
  // nop
}

} // namespace vast

#endif
