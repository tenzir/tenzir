#ifndef VAST_ACTOR_SOURCE_LINE_BASED_H
#define VAST_ACTOR_SOURCE_LINE_BASED_H

#include <istream>

#include "vast/actor/source/base.h"
#include "vast/util/assert.h"

namespace vast {
namespace source {

/// A line-based source that transforms an input stream into lines.
struct line_based_state : state {
  line_based_state(local_actor* self, char const* name)
    : state{self, name},
      input{nullptr} {
  }

  bool next_line() {
    VAST_ASSERT(input && *input);
    if (done_)
      return false;
    line.clear();
    // Get the next non-empty line.
    while (line.empty())
      if (std::getline(*input, line)) {
        ++line_no;
      } else {
        done_ = true;
        return false;
      }
    return true;
  }

  std::unique_ptr<std::istream> input;
  uint64_t line_no = 0;
  std::string line;
};

/// A source that reads input line-by-line.
/// @param self The actor handle.
/// @param sb A streambuffer to read from.
template <typename State>
behavior line_based(stateful_actor<State>* self,
                    std::unique_ptr<std::istream> in) {
  self->state.input = std::move(in);
  return make(self);
};

} // namespace source
} // namespace vast

#endif
