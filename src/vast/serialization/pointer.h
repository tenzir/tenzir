#ifndef VAST_SERIALIZATION_POINTER_H
#define VAST_SERIALIZATION_POINTER_H

#include <memory>
#include "vast/intrusive.h"
#include "vast/object.h"
#include "vast/traits.h"

namespace vast {

// We serialize all pointers as objects because we have to assume reference
// semantics for such types. Otherwise we could just directly serialize the
// pointee. As a result, all pointer-based serializations require announced
// types.

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
  using raw = typename std::remove_pointer<T>::type;
  object o;
  source >> o;
  assert(o.convertible_to<raw>());
  x = o.release_as<raw>();
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
