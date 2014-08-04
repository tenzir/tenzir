#ifndef VAST_SERIALIZATION_FLAT_SET_H
#define VAST_SERIALIZATION_FLAT_SET_H

#include "vast/serialization.h"
#include "vast/util/flat_set.h"

namespace vast {

template <typename T, typename C, typename A>
void serialize(serializer& sink, util::flat_set<T, C, A> const& s)
{
  sink.begin_sequence(s.size());

  for (auto& x : s)
    sink << x;

  sink.end_sequence();
}

template <typename T, typename C, typename A>
void deserialize(deserializer& source, util::flat_set<T, C, A>& s)
{
  uint64_t size;
  source.begin_sequence(size);

  if (size > 0)
  {
    using size_type = typename util::flat_set<T, C, A>::size_type;
    if (size > std::numeric_limits<size_type>::max())
      throw std::length_error("size too large for architecture");
    
    for (size_type i = 0; i < size; ++i)
    {
      T x;
      source >> x;
      s.insert(std::move(x));
    }
  }

  source.end_sequence();
}

} // namespace vast

#endif
