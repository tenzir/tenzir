#include "vast/detail/cppa_type_info.h"

#include <cppa/announce.hpp>
#include "vast/chunk.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/schema.h"
#include "vast/segment.h"
#include "vast/time.h"
#include "vast/uuid.h"

namespace vast {
namespace detail {

template <typename T>
void cppa_announce()
{
  cppa::announce(typeid(T), new cppa_type_info<T>);
}

void cppa_announce_types()
{
  cppa_announce<time_range>();
  cppa_announce<time_point>();
  cppa_announce<uuid>();
  cppa_announce<event>();
  cppa_announce<chunk>();
  cppa_announce<segment>();

  cppa_announce<expression>();
  cppa_announce<schema>();

  cppa_announce<std::vector<uuid>>();
  cppa_announce<std::vector<event>>();
}

} // namespace detail
} // namespace vast
