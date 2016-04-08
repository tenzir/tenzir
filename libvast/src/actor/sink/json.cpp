#include <iterator>
#include <ostream>

#include "vast/actor/sink/json.hpp"
#include "vast/concept/convertible/vast/event.hpp"
#include "vast/concept/convertible/to.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/json.hpp"
#include "vast/util/assert.hpp"

namespace vast {
namespace sink {

json_state::json_state(local_actor* self)
  : state{self, "json-sink"} {
}

json_state::~json_state() {
  *out << "\n]\n";
}

bool json_state::process(event const& e) {
  vast::json j;
  if (!convert(flatten ? vast::flatten(e) : e, j))
    return false;
  auto i = std::ostreambuf_iterator<char>{*out};
  if (first)
    first = false;
  else if (!print(i, ",\n"))
    return false;
  return json_printer<policy::tree, 2, 2>{}.print(i, j);
}

void json_state::flush() {
  out->flush();
}

behavior json(stateful_actor<json_state>* self,
              std::unique_ptr<std::ostream> out, bool flatten) {
  VAST_ASSERT(out != nullptr);
  self->state.out = std::move(out);
  self->state.flatten = flatten;
  *self->state.out << "[\n";
  return make(self);
}

} // namespace sink
} // namespace vast
