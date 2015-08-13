#ifndef VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_RADIX_TREE_H
#define VAST_CONCEPT_SERIALIZABLE_VAST_UTIL_RADIX_TREE_H

#include "vast/concept/serializable/std/pair.h"
#include "vast/util/radix_tree.h"

namespace vast {

// FIXME: serialize the radix tree in its tree form, as opposed to expanding
// each key. The current approach can inflate the serialized represenation
// significantly.

template <typename Serializer, typename T, size_t N>
void serialize(Serializer& sink, util::radix_tree<T, N> const& rt) {
  sink.begin_sequence(rt.size());
  for (auto pair : rt)
    sink << pair;
  sink.end_sequence();
}

template <typename Deserializer, typename T, size_t N>
void deserialize(Deserializer& source, util::radix_tree<T, N>& rt) {
  using mutable_value_type = std::pair<
    typename util::radix_tree<T, N>::key_type,
    typename util::radix_tree<T, N>::mapped_type
  >;
  auto size = source.begin_sequence();
  for (uint64_t i = 0; i < size; ++i) {
    mutable_value_type pair;
    source >> pair;
    rt.insert(std::move(pair));
  }
  source.end_sequence();
}

} // namespace vast

#endif
