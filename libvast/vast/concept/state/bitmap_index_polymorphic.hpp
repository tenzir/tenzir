#ifndef VAST_CONCEPT_STATE_BITMAP_INDEX_POLYMORPHIC_HPP
#define VAST_CONCEPT_STATE_BITMAP_INDEX_POLYMORPHIC_HPP

#include "vast/bitmap_index_polymorphic.hpp"
#include "vast/concept/state/bitmap_index.hpp"
#include "vast/util/meta.hpp"

namespace vast {

template <typename Bitstream>
struct access::state<detail::bitmap_index_model<Bitstream>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.bmi_);
  }
};

template <typename Bitstream>
struct access::state<bitmap_index<Bitstream>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    f(x.concept_);
  }
};

template <typename Bitstream>
struct access::state<sequence_bitmap_index<Bitstream>> {
  template <typename T, typename F>
  static void call(T&& x, F f) {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x), x.elem_type_, x.bmis_, x.size_);
  }
};

} // namespace vast

#endif
