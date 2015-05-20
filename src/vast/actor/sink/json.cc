#include "vast/actor/sink/json.h"
#include "vast/io/algorithm.h"
#include "vast/util/assert.h"
#include "vast/util/json.h"


namespace vast {
namespace sink {

json::json(std::unique_ptr<io::output_stream> out)
  : base<json>{"json-sink"},
    out_{std::move(out)}
{
  VAST_ASSERT(out_ != nullptr);
}

bool json::process(event const& e)
{
  auto j = to<util::json>(e);
  if (! j)
    return false;
  auto str = to_string(*j, true);
  str += '\n';
  return io::copy(str.begin(), str.end(), *out_);
}

void json::flush()
{
  out_->flush();
}

} // namespace sink
} // namespace vast
