#ifndef ZE_CPPA_TYPE_INFO_H
#define ZE_CPPA_TYPE_INFO_H

#include <ze/forward.h>
#include <cppa/util/abstract_uniform_type_info.hpp>

namespace ze {
namespace cppa {

class event_type_info : public ::cppa::util::abstract_uniform_type_info<event>
{
protected:
    void serialize(void const* ptr, ::cppa::serializer* sink) const;
    void deserialize(void* ptr, ::cppa::deserializer* source) const;
};

} // namespace cppa
} // namespace ze

#endif
