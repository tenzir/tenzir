#ifndef VAST_CONCEPT_STATE_BITVECTOR_H
#define VAST_CONCEPT_STATE_BITVECTOR_H

#include "vast/access.h"

namespace vast {

class bitvector;

template <>
struct access::state<bitvector> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.num_bits_, x.bits_);
  }
};

} // namespace vast

#endif
