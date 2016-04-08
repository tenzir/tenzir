#ifndef VAST_CONCEPT_STATE_NONE_HPP
#define VAST_CONCEPT_STATE_NONE_HPP

#include "vast/none.hpp"

namespace vast {

template <>
struct access::state<none> {
  template <typename T, typename F>
  static void call(T&&, F) {
    // nop
  }
};

} // namespace vast

#endif
