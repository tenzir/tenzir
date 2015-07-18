#include <iterator>

#include "vast/actor/sink/ascii.h"
#include "vast/io/iterator.h"
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
  auto i = io::output_iterator{*out_};
  return print(i, e) && print(i, '\n');
}

void ascii::flush()
{
  out_->flush();
}

} // namespace sink
} // namespace vast
