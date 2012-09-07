#include "vast/detail/cppa_type_info.h"

#include <ze/chunk.h>
#include <ze/event.h>
#include <ze/uuid.h>
#include <cppa/announce.hpp>
#include "vast/detail/cppa_archive.h"
#include "vast/schema.h"
#include "vast/segment.h"
#include "vast/to_string.h"
#include "vast/expression.h"

namespace vast {
namespace detail {

using namespace cppa;

void cppa_announce_types()
{
  announce(typeid(ze::uuid), new uuid_type_info);
  announce<std::vector<ze::uuid>>();
  announce(typeid(ze::event), new event_type_info);
  announce<std::vector<ze::event>>();
  announce(typeid(ze::chunk<ze::event>), new event_chunk_type_info);

  announce(typeid(expression), new expression_type_info);
  announce(typeid(segment), new segment_type_info);
  announce(typeid(schema), new schema_type_info);
}

void uuid_type_info::serialize(void const* ptr, serializer* sink) const
{
    auto uuid = reinterpret_cast<ze::uuid const*>(ptr);
    cppa_oarchive oa(sink, name());
    oa << *uuid;
}

void uuid_type_info::deserialize(void* ptr, deserializer* source) const
{
    assert_type_name(source);
    auto uuid = reinterpret_cast<ze::uuid*>(ptr);
    cppa_iarchive ia(source, name());
    ia >> *uuid;
}

void event_type_info::serialize(void const* ptr, serializer* sink) const
{
    auto e = reinterpret_cast<ze::event const*>(ptr);
    cppa_oarchive oa(sink, name());
    oa << *e;
}

void event_type_info::deserialize(void* ptr, deserializer* source) const
{
    assert_type_name(source);
    auto e = reinterpret_cast<ze::event*>(ptr);
    cppa_iarchive ia(source, name());
    ia >> *e;
}

void event_chunk_type_info::serialize(void const* ptr, serializer* sink) const
{
    auto chk = reinterpret_cast<ze::chunk<ze::event> const*>(ptr);
    cppa_oarchive oa(sink, name());
    oa << *chk;
}

void event_chunk_type_info::deserialize(void* ptr, deserializer* source) const
{
    assert_type_name(source);
    auto chk = reinterpret_cast<ze::chunk<ze::event>*>(ptr);
    cppa_iarchive ia(source, name());
    ia >> *chk;
}

void segment_type_info::serialize(void const* ptr, serializer* sink) const
{
    auto s = reinterpret_cast<segment const*>(ptr);
    cppa_oarchive oa(sink, name());
    oa << *s;
}

void segment_type_info::deserialize(void* ptr, deserializer* source) const
{
    assert_type_name(source);
    auto s = reinterpret_cast<segment*>(ptr);
    cppa_iarchive ia(source, name());
    ia >> *s;
}

void expression_type_info::serialize(void const* ptr, serializer* sink) const
{
    auto expr = reinterpret_cast<expression const*>(ptr);
    cppa_oarchive oa(sink, name());
    oa << *expr;
}

void expression_type_info::deserialize(void* ptr, deserializer* source) const
{
    assert_type_name(source);
    auto expr = reinterpret_cast<expression*>(ptr);
    cppa_iarchive ia(source, name());
    ia >> *expr;
}

void schema_type_info::serialize(void const* ptr, serializer* sink) const
{
    auto sch = reinterpret_cast<schema const*>(ptr);
    cppa_oarchive oa(sink, name());
    oa << *sch;
}

void schema_type_info::deserialize(void* ptr, deserializer* source) const
{
    assert_type_name(source);
    auto sch = reinterpret_cast<schema*>(ptr);
    cppa_iarchive ia(source, name());
    ia >> *sch;
}

} // namespace detail
} // namespace vast
