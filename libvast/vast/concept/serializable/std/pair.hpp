#ifndef VAST_CONCEPT_SERIALIZABLE_STD_PAIR_HPP
#define VAST_CONCEPT_SERIALIZABLE_STD_PAIR_HPP

#include <utility>

#include "vast/concept/serializable/builtin.hpp"

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
