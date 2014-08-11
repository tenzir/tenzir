#ifndef VAST_SERIALIZATION_ENUM_H
#define VAST_SERIALIZATION_ENUM_H

#include <type_traits>
#include "vast/serialization/arithmetic.h"

namespace vast {

template <typename T>
std::enable_if_t<std::is_enum<T>::value>
serialize(serializer& sink, T x)
{
  sink << static_cast<std::underlying_type_t<T>>(x);
}

template <typename T>
std::enable_if_t<std::is_enum<T>::value>
deserialize(deserializer& source, T& x)
{
  std::underlying_type_t<T> u;
  source >> u;
  x = static_cast<T>(u);
}

} // namespace vast

#endif
