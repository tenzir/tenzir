#include "vast/detail/cppa_type_info.h"

#include <ze/chunk.h>
#include <ze/event.h>
#include <ze/uuid.h>
#include <cppa/announce.hpp>
#include "vast/schema.h"
#include "vast/segment.h"
#include "vast/to_string.h"
#include "vast/expression.h"

namespace vast {
namespace detail {

template <typename T>
void cppa_announce()
{
  cppa::announce(typeid(T), new cppa_type_info<T>);
}

void cppa_announce_types()
{
  cppa_announce<ze::uuid>();
  cppa_announce<ze::event>();
  cppa_announce<ze::chunk<ze::event>>();

  cppa_announce<expression>();
  cppa_announce<segment>();
  cppa_announce<schema>();

  cppa::announce<std::vector<ze::uuid>>();
  cppa::announce<std::vector<ze::event>>();
}

} // namespace detail
} // namespace vast
