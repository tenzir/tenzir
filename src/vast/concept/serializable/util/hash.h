#ifndef VAST_CONCEPT_SERIALIZABLE_UTIL_HASH_H
#define VAST_CONCEPT_SERIALIZABLE_UTIL_HASH_H

#include "vast/util/hash/xxhash.h"

namespace vast {

template <typename Serializer>
void serialize(Serializer& sink, util::xxhash const& x)
{
  sink.write(&x.state(), sizeof(util::xxhash::state_type));
}

template <typename Deserializer>
void deserialize(Deserializer& source, util::xxhash& x)
{
  util::xxhash::state_type state;
  source.read(&state, sizeof(util::xxhash::state_type));
  x = util::xxhash{state};
}

} // namespace vast

#endif
