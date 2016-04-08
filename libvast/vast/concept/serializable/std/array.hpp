#ifndef VAST_CONCEPT_SERIALIZABLE_STD_ARRAY_HPP
#define VAST_CONCEPT_SERIALIZABLE_STD_ARRAY_HPP

#include <array>
#include <type_traits>

#include "vast/concept/serializable/builtin.hpp"

namespace vast {

template <typename Serializer, typename T, size_t N>
auto serialize(Serializer& sink, std::array<T, N> const& a)
  -> std::enable_if_t<sizeof(T) == 1> {
  sink.write(&a, N);
}

template <typename Deserializer, typename T, size_t N>
auto deserialize(Deserializer& source, std::array<T, N>& a)
  -> std::enable_if_t<sizeof(T) == 1> {
  source.read(&a, N);
}

template <typename Serializer, typename T, size_t N>
auto serialize(Serializer& sink, std::array<T, N> const& a)
  -> std::enable_if_t<sizeof(T) != 1> {
  for (auto& x : a)
    sink << x;
}

template <typename Deserializer, typename T, size_t N>
auto deserialize(Deserializer& source, std::array<T, N>& a)
  -> std::enable_if_t<sizeof(T) != 1> {
  for (auto& x : a)
    source >> x;
}

} // namespace vast

#endif
