#ifndef VAST_CONCEPT_STATE_CHUNK_H
#define VAST_CONCEPT_STATE_CHUNK_H

#include "vast/concept/state/bitstream.h"
#include "vast/concept/state/block.h"
#include "vast/concept/state/time.h"
#include "vast/chunk.h"

namespace vast {

template <>
struct access::state<chunk::meta_data> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.first, x.last, x.ids, x.schema);
  }
};

template <>
struct access::state<chunk> {
  template <typename T, typename F>
  static void read(T const& x, F f) {
    f(x.meta(), x.block());
  }

  template <typename T, typename F>
  static void write(T& x, F f) {
    f(x.get_meta(), x.block());
  }
};

} // namespace vast

#endif
