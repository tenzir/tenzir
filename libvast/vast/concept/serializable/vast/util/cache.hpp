#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_CACHE
#define VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_CACHE

#include <caf/deserializer.hpp>
#include <caf/serializer.hpp>

#include "vast/util/cache.hpp"

namespace vast {
namespace util {

template <class Key, class Value, template <class> class Policy>
void serialize(caf::serializer& sink, cache<Key, Value, Policy> const& c) {
  sink << static_cast<uint32_t>(c.capacity());
  auto size = c.size();
  sink.begin_sequence(size);
  for (auto entry : c)
    sink << entry.first << entry.second;
  sink.end_sequence();
}

template <class Key, class Value, template <class> class Policy>
void serialize(caf::deserializer& source, cache<Key, Value, Policy>& c) {
  uint32_t capacity;
  source >> capacity;
  c.capacity(capacity);
  size_t size;
  source.begin_sequence(size);
  Key k;
  Value v;
  for (uint64_t i = 0; i < size; ++i) {
    source >> k >> v;
    c.insert(std::move(k), std::move(v));
  }
  source.end_sequence();
}

} // namespace vast
} // namespace util

#endif
