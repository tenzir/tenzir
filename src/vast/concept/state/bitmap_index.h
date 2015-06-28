#ifndef VAST_CONCEPT_STATE_BITMAP_INDEX_H
#define VAST_CONCEPT_STATE_BITMAP_INDEX_H

#include "vast/bitmap_index.h"
#include "vast/concept/state/bitmap.h"
#include "vast/concept/state/time.h"
#include "vast/util/meta.h"

namespace vast {

template <typename Derived, typename Bitstream>
struct access::state<bitmap_index_base<Derived, Bitstream>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    f(x.mask_, x.nil_);
  }
};

template <typename Bitstream, typename Z, typename Binner>
struct access::state<arithmetic_bitmap_index<Bitstream, Z, Binner>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x), x.bitmap_);
  }
};

template <typename Bitstream>
struct access::state<string_bitmap_index<Bitstream>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x), x.bitmaps_, x.length_);
  }
};

template <typename Bitstream>
struct access::state<address_bitmap_index<Bitstream>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x), x.bitmaps_, x.v4_);
  }
};

template <typename Bitstream>
struct access::state<subnet_bitmap_index<Bitstream>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x), x.network_, x.length_);
  }
};

template <typename Bitstream>
struct access::state<port_bitmap_index<Bitstream>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x), x.num_, x.proto_);
  }
};

} // namespace vast

#endif
