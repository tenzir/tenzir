#ifndef VAST_CONCEPT_STATE_BLOCK_HPP
#define VAST_CONCEPT_STATE_BLOCK_HPP

#include "vast/access.hpp"

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
