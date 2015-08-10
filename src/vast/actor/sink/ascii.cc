#include <iterator>
#include <ostream>

#include "vast/actor/sink/ascii.h"
#include "vast/util/assert.h"

namespace vast {
namespace sink {

ascii::ascii(std::unique_ptr<std::ostream> out)
  : base<ascii>{"ascii-sink"}, out_{std::move(out)} {
  VAST_ASSERT(out_ != nullptr);
}

bool ascii::process(event const& e) {
  auto i = std::ostreambuf_iterator<std::ostream::char_type>{*out_};
  return print(i, e) && print(i, '\n');
}

void ascii::flush() {
  out_->flush();
}

} // namespace sink
} // namespace vast
