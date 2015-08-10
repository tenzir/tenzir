#ifndef VAST_CONCEPT_STATE_BITMAP_H
#define VAST_CONCEPT_STATE_BITMAP_H

#include "vast/access.h"
#include "vast/concept/state/bitstream.h"
#include "vast/util/meta.h"

namespace vast {

template <typename Bitstream>
struct access::state<singleton_coder<Bitstream>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.bitstream_);
  }
};

template <typename Derived, typename Bitstream>
struct access::state<vector_coder<Derived, Bitstream>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.rows_, x.bitstreams_);
  }
};

template <typename Bitstream>
struct access::state<equality_coder<Bitstream>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x));
  }
};

template <typename Bitstream>
struct access::state<range_coder<Bitstream>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x));
  }
};

template <typename Bitstream>
struct access::state<bitslice_coder<Bitstream>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x));
  }
};

template <typename Base, typename Coder>
struct access::state<multi_level_coder<Base, Coder>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.coders_);
  }
};

template <typename Z, typename Coder, typename Binner>
struct access::state<bitmap<Z, Coder, Binner>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.coder_);
  }
};

} // namespace vast

#endif
