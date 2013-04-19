#ifndef VAST_DETAIL_CPPA_TYPE_INFO_H
#define VAST_DETAIL_CPPA_TYPE_INFO_H

#include <cppa/util/abstract_uniform_type_info.hpp>
#include <ze/fwd.h>
#include "vast/detail/cppa_serialization.h"

namespace vast {
namespace detail {

template <typename T>
class cppa_type_info : public cppa::util::abstract_uniform_type_info<T>
{
protected:
  void serialize(void const* ptr, cppa::serializer* sink) const final
  {
    auto x = reinterpret_cast<T const*>(ptr);
    cppa_serializer serializer(sink, this->name());
    serializer << *x;
  }

  void deserialize(void* ptr, cppa::deserializer* source) const final
  {
    this->assert_type_name(source);
    auto x = reinterpret_cast<T*>(ptr);
    cppa_deserializer deserializer(source, this->name());
    deserializer >> *x;
  }
};

/// Announces all necessary types we use in VAST.
void cppa_announce_types();

} // namespace detail
} // namespace vast

#endif
