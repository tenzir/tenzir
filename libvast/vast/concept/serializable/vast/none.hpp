#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_NONE_HPP
#define VAST_CONCEPT_SERIALIZABLE_VAST_NONE_HPP

#include "vast/none.hpp"

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
