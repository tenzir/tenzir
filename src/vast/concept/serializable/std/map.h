#ifndef VAST_CONCEPT_SERIALIZABLE_MAP_H
#define VAST_CONCEPT_SERIALIZABLE_MAP_H

#include <map>

#include "vast/concept/serializable/std/pair.h"

namespace vast {

template <typename Serializer, typename Key, typename T>
void serialize(Serializer& sink, std::map<Key, T> const& map) {
  sink.begin_sequence(map.size());
  for (auto& p : map)
    sink << p;
  sink.end_sequence();
}

template <typename Deserializer, typename Key, typename T>
void deserialize(Deserializer& source, std::map<Key, T>& map) {
  auto size = source.begin_sequence();
  map.clear();
  for (uint64_t i = 0; i < size; ++i) {
    std::pair<Key, T> p;
    source >> p;
    map.insert(std::move(p));
  }
  source.end_sequence();
}

} // namespace vast

#endif
