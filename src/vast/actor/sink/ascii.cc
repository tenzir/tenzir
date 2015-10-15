#include <iterator>
#include <ostream>

#include "vast/actor/sink/ascii.h"
#include "vast/util/assert.h"

namespace vast {
namespace sink {

ascii_state::ascii_state(local_actor* self)
  : state{self, "ascii-sink"} {
}

bool ascii_state::process(event const& e) {
  auto i = std::ostreambuf_iterator<char>{*out};
  return print(i, e) && print(i, '\n');
}

void ascii_state::flush() {
  out->flush();
}

behavior ascii(stateful_actor<ascii_state>* self,
               std::unique_ptr<std::ostream> out) {
  VAST_ASSERT(out != nullptr);
  self->state.out = move(out);
  return make(self);
}

} // namespace sink
} // namespace vast
