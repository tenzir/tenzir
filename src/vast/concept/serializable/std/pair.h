#ifndef VAST_CONCEPT_SERIALIZABLE_STD_PAIR_H
#define VAST_CONCEPT_SERIALIZABLE_STD_PAIR_H

#include <utility>

#include "vast/concept/serializable/builtin.h"

namespace vast {

template <typename Serializer, typename T, typename U>
void serialize(Serializer& sink, std::pair<T, U> const& p) {
  sink << p.first << p.second;
}

template <typename Deserializer, typename T, typename U>
void deserialize(Deserializer& source, std::pair<T, U>& p) {
  source >> p.first >> p.second;
}

} // namespace vast

#endif
