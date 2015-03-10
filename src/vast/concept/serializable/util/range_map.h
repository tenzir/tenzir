#ifndef VAST_CONCEPT_SERIALIZABLE_UTIL_RANGE_MAP_H
#define VAST_CONCEPT_SERIALIZABLE_UTIL_RANGE_MAP_H

#include "vast/util/range_map.h"

namespace vast {

template <typename Serializer, typename P, typename V>
void serialize(Serializer& sink, util::range_map<P, V> const& rm)
{
  sink.begin_sequence(rm.size());
  for (auto t : rm)
    sink << get<0>(t) << get<1>(t) << get<2>(t);
  sink.end_sequence();
}

template <typename Deserializer, typename P, typename V>
void deserialize(Deserializer& source, util::range_map<P, V>& rm)
{
  auto size = source.begin_sequence();
  for (uint64_t i = 0; i < size; ++i)
  {
    P l, r;
    V v;
    source >> l >> r >> v;
    rm.insert(std::move(l), std::move(r), std::move(v));
  }
  source.end_sequence();
}

} // namespace vast

#endif
