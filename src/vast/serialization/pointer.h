#ifndef VAST_SERIALIZATION_POINTER_H
#define VAST_SERIALIZATION_POINTER_H

// If we encounter a pointer, we assume that the element type has reference
// semantics and may exhibit runtime polymorphism. (Otherwise we could directly
// serialize the pointee.) Therefore, all pointer-based serializations require
// announced types.

#include "vast/serialization.h"

namespace vast {

template <typename T>
std::enable_if_t<util::is_ptr<T>::value>
serialize(serializer& sink, T const& x)
{
  write_object(sink, *x);
}

template <typename T>
std::enable_if_t<std::is_pointer<T>::value>
deserialize(deserializer& source, T& x)
{
  object o;
  source >> o;
  x = o.release_as<std::remove_pointer_t<T>>();
}

template <typename T>
std::enable_if_t<util::is_smart_ptr<T>::value>
deserialize(deserializer& source, T& x)
{
  typename T::element_type* e;
  source >> e;
  x = T{e};
}

} // namespace vast

#endif
