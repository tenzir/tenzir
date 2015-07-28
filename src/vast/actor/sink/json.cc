#include <iterator>
#include <ostream>

#include "vast/actor/sink/json.h"
#include "vast/concept/convertible/vast/event.h"
#include "vast/concept/convertible/to.h"
#include "vast/concept/printable/print.h"
#include "vast/concept/printable/vast/event.h"
#include "vast/concept/printable/vast/json.h"
#include "vast/util/assert.h"

namespace vast {
namespace sink {

json::json(std::unique_ptr<std::ostream> out)
  : base<json>{"json-sink"},
    out_{std::move(out)}
{
  VAST_ASSERT(out_ != nullptr);
}

bool json::process(event const& e)
{
  auto j = to<vast::json>(e);
  if (! j)
    return false;
  auto i = std::ostreambuf_iterator<std::ostream::char_type>{*out_};
  return print(i, *j) && print(i, '\n');
}

void json::flush()
{
  out_->flush();
}

} // namespace sink
} // namespace vast
