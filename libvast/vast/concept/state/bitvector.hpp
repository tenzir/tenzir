#ifndef VAST_CONCEPT_STATE_BITVECTOR_HPP
#define VAST_CONCEPT_STATE_BITVECTOR_HPP

#include "vast/access.hpp"

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
