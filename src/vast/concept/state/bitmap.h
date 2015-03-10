#ifndef VAST_CONCEPT_STATE_BITMAP_H
#define VAST_CONCEPT_STATE_BITMAP_H

#include "vast/access.h"
#include "vast/concept/state/bitstream.h"
#include "vast/util/meta.h"

namespace vast {

template <typename Derived>
struct access::state<coder<Derived>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    f(x.rows_);
  }
};

template <typename Z, typename Bitstream>
struct access::state<equality_coder<Z, Bitstream>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x), x.bitstreams_);
  }
};

template <typename Z, typename Bitstream>
struct access::state<binary_bitslice_coder<Z, Bitstream>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x), x.bitstreams_);
  }
};

template <typename Derived, typename Z, typename Bitstream>
struct access::state<bitslice_coder<Derived, Z, Bitstream>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x), x.base_, x.v_, x.bitstreams_);
  }
};

template <typename Z, typename Bitstream>
struct access::state<equality_bitslice_coder<Z, Bitstream>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x));
  }
};

template <typename Z, typename Bitstream>
struct access::state<range_bitslice_coder<Z, Bitstream>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    using super = typename std::decay_t<decltype(x)>::super;
    using base = util::deduce<decltype(x), super>;
    f(static_cast<base>(x));
  }
};

template <typename Z>
struct access::state<null_binner<Z>>
{
  template <typename T, typename F>
  static void call(T&&, F)
  {
    // nop
  }
};

template <typename Z>
struct access::state<precision_binner<Z>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    f(x.integral_, x.fractional_);
  }
};

template <
  typename Z,
  typename BS,
  template <typename, typename> class C,
  template <typename> class B
>
struct access::state<bitmap<Z, BS, C, B>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    f(x.binner_, x.coder_);
  }
};

template <
  typename BS,
  template <typename, typename> class C,
  template <typename> class B
>
struct access::state<bitmap<bool, BS, C, B>>
{
  template <typename T, typename F>
  static void call(T&& x, F f)
  {
    f(x.bool_);
  }
};

} // namespace vast

#endif
