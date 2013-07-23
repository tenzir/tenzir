#ifndef VAST_SERIALIZATION_POINTER_H
#define VAST_SERIALIZATION_POINTER_H

#include <memory>
#include "vast/intrusive.h"
#include "vast/traits.h"

namespace vast {

template <typename T>
typename std::enable_if<is_pointer_type<T>::value>::type
serialize(serializer& sink, T const& x)
{
  sink << *x;
}

template <typename T>
void deserialize(deserializer& source, std::unique_ptr<T>& x)
{
  if (! x)
    x.reset(new T());
  source >> *x;
}

template <typename T>
void deserialize(deserializer& source, std::shared_ptr<T>& x)
{
  if (! x)
    x = std::make_shared<T>();
  source >> *x;
}

template <typename T>
void deserialize(deserializer& source, intrusive_ptr<T>& x)
{
  if (! x)
    x = new T();
  source >> *x;
}

} // namespace vast

#endif
