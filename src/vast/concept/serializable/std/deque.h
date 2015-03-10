#ifndef VAST_CONCEPT_SERIALIZABLE_STD_DEQUE_H
#define VAST_CONCEPT_SERIALIZABLE_STD_DEQUE_H

#include <type_traits>
#include <deque>
#include "vast/concept/serializable/builtin.h"

namespace vast {

template <typename Serializer, typename T, typename Allocator>
void serialize(Serializer& sink, std::deque<T, Allocator> const& d)
{
  sink.begin_sequence(d.size());
  for (auto const& x : d)
    sink << x;
  sink.end_sequence();
}

template <typename Deserializer, typename T, typename Allocator>
void deserialize(Deserializer& source, std::deque<T, Allocator>& d)
{
  auto size = source.begin_sequence();
  if (size > 0)
  {
    d.resize(size);
    for (auto& x : d)
      source >> x;
  }
  source.end_sequence();
}

} // namespace vast

#endif

