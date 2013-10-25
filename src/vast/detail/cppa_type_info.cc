#include "vast/detail/cppa_type_info.h"

#include <cppa/cppa.hpp>
#include "vast/bitstream.h"
#include "vast/chunk.h"
#include "vast/event.h"
#include "vast/expression.h"
#include "vast/operator.h"
#include "vast/regex.h"
#include "vast/schema.h"
#include "vast/search_result.h"
#include "vast/segment.h"
#include "vast/string.h"
#include "vast/time.h"
#include "vast/uuid.h"
#include "vast/value.h"
#include "vast/util/make_unique.h"

namespace vast {
namespace detail {

template <typename T>
void cppa_announce()
{
  cppa::announce(typeid(T), make_unique<cppa_type_info<T>>());
}

void cppa_announce_types()
{
  cppa_announce<time_range>();
  cppa_announce<time_point>();
  cppa_announce<string>();
  cppa_announce<regex>();
  cppa_announce<value>();

  cppa_announce<uuid>();
  cppa_announce<std::vector<uuid>>();
  cppa_announce<record>();
  cppa_announce<offset>();
  cppa_announce<event>();
  cppa_announce<std::vector<event>>();
  cppa_announce<chunk>();
  cppa_announce<segment>();

  cppa_announce<arithmetic_operator>();
  cppa_announce<boolean_operator>();
  cppa_announce<relational_operator>();
  cppa_announce<expr::ast>();
  cppa_announce<schema>();
  cppa_announce<search_result>();

  cppa_announce<null_bitstream>();
  cppa_announce<bitstream>();
}

} // namespace detail
} // namespace vast
