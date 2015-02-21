#include "vast/actor/sink/json.h"
#include "vast/util/json.h"

namespace vast {
namespace sink {

json::json(path p)
  : base<json>{"json-sink"},
    stream_{std::move(p)}
{
}

bool json::process(event const& e)
{
  auto j = to<util::json>(e);
  if (! j)
    return false;

  auto str = to_string(*j, true);
  str += '\n';

  return stream_.write(str.begin(), str.end());
}

} // namespace sink
} // namespace vast
