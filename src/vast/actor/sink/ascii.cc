#include "vast/actor/sink/ascii.h"
#include "vast/io/algorithm.h"
#include "vast/util/assert.h"

namespace vast {
namespace sink {

ascii::ascii(std::unique_ptr<io::output_stream> out)
  : base<ascii>{"ascii-sink"},
    out_{std::move(out)}
{
  VAST_ASSERT(out_ != nullptr);
}

bool ascii::process(event const& e)
{
  auto str = to_string(e) + '\n';
  return io::copy(str.begin(), str.end(), *out_);
}

void ascii::flush()
{
  out_->flush();
}

} // namespace sink
} // namespace vast
