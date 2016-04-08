#ifndef VAST_CONCEPT_SERIALIZABLE_STD_UNORDERED_MAP_HPP
#define VAST_CONCEPT_SERIALIZABLE_STD_UNORDERED_MAP_HPP

#include <unordered_map>

#include "vast/concept/serializable/std/pair.hpp"

namespace vast {

template <typename Serializer, typename Key, typename T>
void serialize(Serializer& sink, std::unordered_map<Key, T> const& map) {
  sink.begin_sequence(map.size());
  for (auto& i : map)
    serialize(sink, i);
  sink.end_sequence();
}

template <typename Deserializer, typename Key, typename T>
void deserialize(Deserializer& source, std::unordered_map<Key, T>& map) {
  auto size = source.begin_sequence();
  map.clear();
  map.reserve(size);
  for (uint64_t i = 0; i < size; ++i) {
    std::pair<Key, T> p;
    deserialize(source, p);
    map.insert(std::move(p));
  }
  source.end_sequence();
}

} // namespace vast

#endif
