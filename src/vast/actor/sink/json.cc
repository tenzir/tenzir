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

json_state::json_state(local_actor* self)
  : state{self, "json-sink"} {
}

bool json_state::process(event const& e) {
  auto j = to<vast::json>(e);
  if (!j)
    return false;
  auto i = std::ostreambuf_iterator<char>{*out};
  return print(i, *j) && print(i, '\n');
}

void json_state::flush() {
  out->flush();
}

behavior json(stateful_actor<json_state>* self, std::ostream* out) {
  VAST_ASSERT(out != nullptr);
  self->state.out = std::unique_ptr<std::ostream>{out};
  return make(self);
}

} // namespace sink
} // namespace vast
