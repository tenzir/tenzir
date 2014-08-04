#ifndef VAST_SERIALIZATION_ENUM_H
#define VAST_SERIALIZATION_ENUM_H

#include "vast/serialization/arithmetic.h"

namespace vast {

template <typename T>
std::enable_if_t<std::is_enum<T>::value>
serialize(serializer& sink, T&& x)
{
  sink << static_cast<typename std::underlying_type<T>::type>(x);
}

template <typename T>
std::enable_if_t<std::is_enum<T>::value>
deserialize(deserializer& source, T& x)
{
  typename std::underlying_type<T>::type underlying;
  source >> underlying;
  x = static_cast<T>(underlying);
}

} // namespace vast

#endif
