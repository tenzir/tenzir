#ifndef VAST_SERIALIZATION_POINTER_H
#define VAST_SERIALIZATION_POINTER_H

#include <memory>
#include "vast/object.h"
#include "vast/traits.h"
#include "vast/util/intrusive.h"

namespace vast {

// If we encounter a pointer, we assume that the element type has reference
// semantics and may exhibit runtime polymorphism. (Otherwise we could directly
// serialize the pointee.) As such, all pointer-based serializations require
// announced types.

template <typename T>
EnableIf<is_ptr<T>>
serialize(serializer& sink, T const& x)
{
  write_object(sink, *x);
}

template <typename T>
EnableIf<std::is_pointer<T>>
deserialize(deserializer& source, T& x)
{
  object o;
  source >> o;
  x = o.release_as<typename std::remove_pointer<T>::type>();
}

template <typename T>
EnableIf<is_smart_ptr<T>>
deserialize(deserializer& source, T& x)
{
  typename T::element_type* e;
  source >> e;
  x = T{e};
}

} // namespace vast

#endif
