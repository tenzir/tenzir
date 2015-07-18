#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_FLAT_SET_H
#define VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_FLAT_SET_H

#include "vast/util/flat_set.h"

namespace vast {

template <typename Serializer, typename T, typename C, typename A>
void serialize(Serializer& sink, util::flat_set<T, C, A> const& s)
{
  sink.begin_sequence(s.size());
  for (auto& x : s)
    sink << x;
  sink.end_sequence();
}

template <typename Deserializer, typename T, typename C, typename A>
void deserialize(Deserializer& source, util::flat_set<T, C, A>& s)
{
  auto size = source.begin_sequence();
  for (uint64_t i = 0; i < size; ++i)
  {
    T x;
    source >> x;
    s.insert(std::move(x));
  }
  source.end_sequence();
}

} // namespace vast

#endif
