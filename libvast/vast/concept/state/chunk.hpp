#ifndef VAST_CONCEPT_STATE_CHUNK_HPP
#define VAST_CONCEPT_STATE_CHUNK_HPP

#include "vast/concept/state/bitstream.hpp"
#include "vast/concept/state/block.hpp"
#include "vast/concept/state/time.hpp"
#include "vast/chunk.hpp"

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
