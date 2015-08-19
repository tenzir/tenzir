#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_CACHE
#define VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_CACHE

#include "vast/concept/serializable/builtin.h"
#include "vast/util/cache.h"

namespace vast {
namespace util {

template <typename Serializer, typename Key, typename Value,
          template <typename> class Policy>
void serialize(Serializer& sink, cache<Key, Value, Policy> const& c) {
  sink << static_cast<uint64_t>(c.capacity());
  sink << static_cast<uint64_t>(c.size());
  for (auto entry : c)
    sink << entry.first << entry.second;
}

template <typename Deserializer, typename Key, typename Value,
          template <typename> class Policy>
void deserialize(Deserializer& source, cache<Key, Value, Policy>& c) {
  uint64_t size, capacity;
  source >> capacity >> size;
  c.capacity(capacity);
  Key k;
  Value v;
  for (uint64_t i = 0; i < size; ++i) {
    source >> k >> v;
    c.insert(std::move(k), std::move(v));
  }
}

} // namespace vast
} // namespace util

#endif
