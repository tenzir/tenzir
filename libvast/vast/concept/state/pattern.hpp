#ifndef VAST_CONCEPT_STATE_PATTERN_HPP
#define VAST_CONCEPT_STATE_PATTERN_HPP

#include "vast/access.hpp"

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
