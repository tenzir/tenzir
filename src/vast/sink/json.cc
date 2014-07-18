#include "vast/sink/json.h"
#include "vast/util/json.h"

namespace vast {
namespace sink {

json::json(path p)
  : stream_{std::move(p)}
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

std::string json::describe() const
{
  return "json-sink";
}

} // namespace sink
} // namespace vast
