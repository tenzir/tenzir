#include "vast/actor/sink/ascii.h"

namespace vast {
namespace sink {

ascii::ascii(path p)
  : base<ascii>{"ascii-sink"},
    stream_{std::move(p)}
{
}

bool ascii::process(event const& e)
{
  auto str = to_string(e) + '\n';
  return stream_.write(str.begin(), str.end());
}

} // namespace sink
} // namespace vast
