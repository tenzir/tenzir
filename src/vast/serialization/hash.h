#ifndef VAST_SERIALIZATION_HASH_H
#define VAST_SERIALIZATION_HASH_H

#include "vast/util/hash/xxhash.h"

namespace vast {

inline void serialize(serializer& sink, util::xxhash const& x)
{
  sink.write_raw(&x.state(), sizeof(util::xxhash::state_type));
}

inline void deserialize(deserializer& source, util::xxhash& x)
{
  util::xxhash::state_type state;
  source.read_raw(&state, sizeof(util::xxhash::state_type));
  x = util::xxhash{state};
}

} // namespace vast

#endif
