#ifndef VAST_CONCEPT_STATE_PATTERN_H
#define VAST_CONCEPT_STATE_PATTERN_H

#include "vast/access.h"

namespace vast {

class pattern;

template <>
struct access::state<pattern> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.str_);
  }
};

} // namespace vast

#endif
