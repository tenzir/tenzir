#ifndef VAST_DETAIL_CPPA_TYPE_INFO_H
#define VAST_DETAIL_CPPA_TYPE_INFO_H

#include <caf/detail/abstract_uniform_type_info.hpp>
#include "vast/logger.h"
#include "vast/detail/caf_serialization.h"

namespace vast {
namespace detail {

template <typename T>
class caf_type_info : public caf::detail::abstract_uniform_type_info<T>
{
public:
  caf_type_info(std::string name)
    : caf::detail::abstract_uniform_type_info<T>(std::move(name))
  {
  }

protected:
  void serialize(void const* ptr, caf::serializer* sink) const final
  {
    VAST_ENTER();
    auto x = reinterpret_cast<T const*>(ptr);
    caf_serializer serializer{sink};
    serializer << *x;
  }

  void deserialize(void* ptr, caf::deserializer* source) const final
  {
    VAST_ENTER();
    auto x = reinterpret_cast<T*>(ptr);
    caf_deserializer deserializer{source};
    deserializer >> *x;
  }
};

} // namespace detail
} // namespace vast

#endif
