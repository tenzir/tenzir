#ifndef VAST_SERIALIZATION_RANGE_MAP_H
#define VAST_SERIALIZATION_RANGE_MAP_H

#include "vast/serialization.h"
#include "vast/util/range_map.h"

namespace vast {

template <typename P, typename V>
void serialize(serializer& sink, util::range_map<P, V> const& s)
{
  sink.begin_sequence(s.size());
  s.each([&](P const& l, P const& r, V const& v) { sink << l << r << v; });
  sink.end_sequence();
}

template <typename P, typename V>
void deserialize(deserializer& source, util::range_map<P, V>& s)
{
  uint64_t size;
  source.begin_sequence(size);

  if (size > 0)
    for (uint64_t i = 0; i < size; ++i)
    {
      P l, r;
      V v;
      source >> l >> r >> v;
      s.insert(std::move(l), std::move(r), std::move(v));
    }

  source.end_sequence();
}

} // namespace vast

#endif
