#ifndef VAST_SERIALIZATION_POINTER_H
#define VAST_SERIALIZATION_POINTER_H

#include <memory>
#include "vast/object.h"
#include "vast/util/intrusive.h"
#include "vast/util/meta.h"

namespace vast {

// If we encounter a pointer, we assume that the element type has reference
// semantics and may exhibit runtime polymorphism. (Otherwise we could directly
// serialize the pointee.) As such, all pointer-based serializations require
// announced types.

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
  x = o.release_as<typename std::remove_pointer<T>::type>();
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
