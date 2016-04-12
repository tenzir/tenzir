#ifndef VAST_CONCEPT_STATE_CHUNK_HPP
#define VAST_CONCEPT_STATE_CHUNK_HPP

#include "vast/concept/state/bitstream.hpp"
#include "vast/concept/state/time.hpp"
#include "vast/chunk.hpp"

namespace vast {

template <>
struct access::state<chunk> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.events_, x.first_, x.last_, x.ids_, x.schema_, x.compression_method_,
      x.buffer_);
  }
};

} // namespace vast

#endif
