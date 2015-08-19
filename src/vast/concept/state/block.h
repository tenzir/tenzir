#ifndef VAST_CONCEPT_STATE_BLOCK_H
#define VAST_CONCEPT_STATE_BLOCK_H

#include "vast/access.h"

namespace vast {

class block;

template <>
struct access::state<block> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.compression_, x.elements_, x.uncompressed_bytes_, x.buffer_);
  }
};

} // namespace vast

#endif
