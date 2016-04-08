#ifndef VAST_CONCEPT_SERIALIZABLE_STD_LIST_HPP
#define VAST_CONCEPT_SERIALIZABLE_STD_LIST_HPP

#include <list>
#include "vast/concept/serializable/builtin.hpp"

namespace vast {

template <typename Serializer, typename T>
void serialize(Serializer& sink, std::list<T> const& list) {
  sink.begin_sequence(list.size());
  for (auto& x : list)
    sink << x;
  sink.end_sequence();
}

template <typename Deserializer, typename T>
void deserialize(Deserializer& source, std::list<T>& list) {
  list.clear();
  uint64_t size = source.begin_sequence();
  for (uint64_t i = 0; i < size; ++i) {
    T x;
    source >> x;
    list.push_back(std::move(x));
  }
  source.end_sequence();
}

} // namespace vast

#endif
