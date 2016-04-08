#ifndef VAST_CONCEPT_STATE_BITSTREAM_POLYMORPHIC_HPP
#define VAST_CONCEPT_STATE_BITSTREAM_POLYMORPHIC_HPP

#include "vast/concept/state/bitstream.hpp"

namespace vast {

namespace detail {
template <typename>
class bitstream_model;
} // namespace detail

class bitstream;

template <typename Bitstream>
struct access::state<detail::bitstream_model<Bitstream>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.bitstream_);
  }
};

template <>
struct access::state<bitstream> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.concept_);
  }
};

} // namespace vast

#endif
