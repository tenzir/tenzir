#include "vast/detail/cppa_type_info.h"

#include <ze/chunk.h>
#include <ze/event.h>
#include <cppa/announce.hpp>
#include "vast/detail/cppa_archive.h"
#include "vast/store/segment.h"

namespace vast {
namespace detail {

void cppa_announce_types()
{
  using namespace cppa;
  announce(typeid(ze::uuid), new uuid_type_info);
  announce<std::vector<ze::uuid>>();
  announce(typeid(ze::event), new event_type_info);
  announce<std::vector<ze::event>>();
  announce(typeid(ze::chunk<ze::event>), new event_chunk_type_info);
  announce(typeid(store::segment), new segment_type_info);
}

void uuid_type_info::serialize(void const* ptr, cppa::serializer* sink) const
{
    auto uuid = reinterpret_cast<ze::uuid const*>(ptr);
    cppa_oarchive oa(sink, name());
    oa << *uuid;
}

void uuid_type_info::deserialize(void* ptr, cppa::deserializer* source) const
{
    std::string cname = source->seek_object();
    if (cname != name())
        throw std::logic_error("wrong type name found");

    auto uuid = reinterpret_cast<ze::uuid*>(ptr);
    cppa_iarchive ia(source, cname);
    ia >> *uuid;
}

void event_type_info::serialize(void const* ptr, cppa::serializer* sink) const
{
    auto e = reinterpret_cast<ze::event const*>(ptr);
    cppa_oarchive oa(sink, name());
    oa << *e;
}

void event_type_info::deserialize(void* ptr, cppa::deserializer* source) const
{
    std::string cname = source->seek_object();
    if (cname != name())
        throw std::logic_error("wrong type name found");

    auto e = reinterpret_cast<ze::event*>(ptr);
    cppa_iarchive ia(source, cname);
    ia >> *e;
}

void event_chunk_type_info::serialize(void const* ptr, cppa::serializer* sink) const
{
    auto chk = reinterpret_cast<ze::chunk<ze::event> const*>(ptr);
    cppa_oarchive oa(sink, name());
    oa << *chk;
}

void event_chunk_type_info::deserialize(void* ptr, cppa::deserializer* source) const
{
    std::string cname = source->seek_object();
    if (cname != name())
        throw std::logic_error("wrong type name found");

    auto chk = reinterpret_cast<ze::chunk<ze::event>*>(ptr);
    cppa_iarchive ia(source, cname);
    ia >> *chk;
}

void segment_type_info::serialize(void const* ptr, cppa::serializer* sink) const
{
    auto s = reinterpret_cast<store::segment const*>(ptr);
    cppa_oarchive oa(sink, name());
    oa << *s;
}

void segment_type_info::deserialize(void* ptr, cppa::deserializer* source) const
{
    std::string cname = source->seek_object();
    if (cname != name())
        throw std::logic_error("wrong type name found");

    auto s = reinterpret_cast<store::segment*>(ptr);
    cppa_iarchive ia(source, cname);
    ia >> *s;
}

} // namespace detail
} // namespace vast
