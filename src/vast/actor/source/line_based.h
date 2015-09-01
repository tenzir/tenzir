#ifndef VAST_ACTOR_SOURCE_LINE_BASED_H
#define VAST_ACTOR_SOURCE_LINE_BASED_H

#include <istream>

#include "vast/actor/source/base.h"
#include "vast/util/assert.h"

namespace vast {
namespace source {

/// A line-based source that transforms an input stream into lines.
struct line_based_state : base_state {
  line_based_state(local_actor* self, char const* name)
    : base_state{self, name},
      input{nullptr} {
  }

  bool next_line() {
    VAST_ASSERT(input);
    if (done_)
      return false;
    line.clear();
    // Get the next non-empty line.
    while (line.empty())
      if (std::getline(input, line)) {
        ++line_no;
      } else {
        done_ = true;
        return false;
      }
    return true;
  }

  std::istream input;
  uint64_t line_no = 0;
  std::string line;
};

/// A source that reads input line-by-line.
/// @param self The actor handle.
/// @param sb A streambuffer to read from.
template <typename State>
behavior line_based(stateful_actor<State>* self, std::streambuf* sb) {
  // FIXME: The naked owning pointer is not exception safe. But because CAF's
  // factory function constructing stateful actors currently shoves all
  // arguments into a message, we cannot have non-copyable types as arguments.
  // Once this changes we should switch back to a unique_ptr<std::streambuf>.
  self->state.input.rdbuf(sb);
  return base(self);
};

} // namespace source
} // namespace vast

#endif
