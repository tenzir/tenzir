#ifndef VAST_IO_SERIALIZATION_POINTER_H
#define VAST_IO_SERIALIZATION_POINTER_H

#include <memory>
#include "vast/intrusive.h"
#include "vast/traits.h"

namespace vast {
namespace io {

template <typename T>
typename std::enable_if<is_pointer_type<T>::value>::type
serialize(serializer& sink, T const& x)
{
  sink << *x;
}

template <typename T>
typename std::enable_if<is_unique_ptr<T>::value>::type
deserialize(deserializer& source, T& x)
{
  if (! x)
    x.reset(new typename T::element_type);
  source >> *x;
}

template <typename T>
typename std::enable_if<is_shared_ptr<T>::value>::type
deserialize(deserializer& source, T& x)
{
  if (! x)
    x = std::make_shared<typename T::element_type>();
  source >> *x;
}

template <typename T>
typename std::enable_if<is_intrusive_ptr<T>::value>::type
deserialize(deserializer& source, T& x)
{
  if (! x)
    x = new typename T::element_type;
  source >> *x;
}

} // namespace io
} // namespace vast

#endif
