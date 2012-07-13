#ifndef VAST_DETAIL_CPPA_TYPE_INFO_H
#define VAST_DETAIL_CPPA_TYPE_INFO_H

#include <ze/forward.h>
#include <cppa/util/abstract_uniform_type_info.hpp>

namespace vast {

// Forward declarations.
namespace store {
class segment;
} // namespace store

namespace detail {

/// Announces all necessary types we use here.
void cppa_announce_types();

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

class event_chunk_type_info
  : public cppa::util::abstract_uniform_type_info<ze::chunk<ze::event>>
{
protected:
  void serialize(void const* ptr, cppa::serializer* sink) const;
  void deserialize(void* ptr, cppa::deserializer* source) const;
};

class segment_type_info
  : public cppa::util::abstract_uniform_type_info<store::segment>
{
protected:
  void serialize(void const* ptr, cppa::serializer* sink) const;
  void deserialize(void* ptr, cppa::deserializer* source) const;
};

} // namespace detail
} // namespace vast

#endif
