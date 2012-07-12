#ifndef VAST_DETAIL_CPPA_TYPE_INFO_H
#define VAST_DETAIL_CPPA_TYPE_INFO_H

#include <ze/forward.h>
#include <cppa/util/abstract_uniform_type_info.hpp>

namespace vast {
namespace detail {

class uuid_type_info : public cppa::util::abstract_uniform_type_info<ze::uuid>
{
protected:
  void serialize(void const* ptr, cppa::serializer* sink) const;
  void deserialize(void* ptr, cppa::deserializer* source) const;
};

class event_type_info : public cppa::util::abstract_uniform_type_info<ze::event>
{
protected:
  void serialize(void const* ptr, cppa::serializer* sink) const;
  void deserialize(void* ptr, cppa::deserializer* source) const;
};

} // namespace detail
} // namespace vast

#endif
