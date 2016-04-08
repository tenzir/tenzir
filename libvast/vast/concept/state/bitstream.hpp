#ifndef VAST_CONCEPT_STATE_BITSTREAM_HPP
#define VAST_CONCEPT_STATE_BITSTREAM_HPP

#include "vast/bitstream.hpp"
#include "vast/concept/state/bitvector.hpp"

namespace vast {

template <>
struct access::state<null_bitstream> {
  template <typename Bitstream, typename F>
  static void call(Bitstream&& bs, F f) {
    f(bs.bits_);
  }
};

template <>
struct access::state<ewah_bitstream> {
  template <typename Bitstream, typename F>
  static void call(Bitstream&& bs, F f) {
    f(bs.num_bits_, bs.last_marker_, bs.bits_);
  }
};

} // namespace vast

#endif
