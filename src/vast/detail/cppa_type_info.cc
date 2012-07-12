#include <vast/detail/cppa_type_info.h>

#include <ze/event.h>
#include <vast/detail/cppa_archive.h>

namespace vast {
namespace detail {

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

} // namespace detail
} // namespace vast
