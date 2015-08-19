#ifndef VAST_CONCEPT_SERIALIZABLE_STD_VECTOR_H
#define VAST_CONCEPT_SERIALIZABLE_STD_VECTOR_H

#include <type_traits>
#include <vector>

#include "vast/concept/serializable/builtin.h"

namespace vast {

template <typename Serializer, typename T, typename Allocator>
auto serialize(Serializer& sink, std::vector<T, Allocator> const& v)
  -> std::enable_if_t<sizeof(T) == 1> {
  sink.begin_sequence(v.size());
  if (!v.empty())
    sink.write(v.data(), v.size());
  sink.end_sequence();
}

template <typename Deserializer, typename T, typename Allocator>
auto deserialize(Deserializer& source, std::vector<T, Allocator>& v)
  -> std::enable_if_t<sizeof(T) == 1> {
  auto size = source.begin_sequence();
  if (size > 0) {
    v.resize(size);
    source.read(v.data(), size);
  }
  source.end_sequence();
}

template <typename Serializer, typename T, typename Allocator>
auto serialize(Serializer& sink, std::vector<T, Allocator> const& v)
  -> std::enable_if_t<sizeof(T) != 1> {
  sink.begin_sequence(v.size());
  for (auto const& x : v)
    sink << x;
  sink.end_sequence();
}

template <typename Deserializer, typename T, typename Allocator>
auto deserialize(Deserializer& source, std::vector<T, Allocator>& v)
  -> std::enable_if_t<sizeof(T) != 1> {
  auto size = source.begin_sequence();
  if (size > 0) {
    v.resize(size);
    for (auto& x : v)
      source >> x;
  }
  source.end_sequence();
}

} // namespace vast

#endif
