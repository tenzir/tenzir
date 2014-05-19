#ifndef VAST_UTIL_META_H
#define VAST_UTIL_META_H

namespace vast {
namespace util {

/// Computes the maximum over a variadic list of types according to a given
/// higher-order metafunction.
template <template <typename> class F, typename Head>
constexpr decltype(F<Head>::value) max()
{
  return F<Head>::value;
}

template <
  template <typename> class F, typename Head, typename Next, typename... Tail
>
constexpr decltype(F<Head>::value) max()
{
  return max<F, Head>() > max<F, Next, Tail...>()
    ? max<F, Head>()
    : max<F, Next, Tail...>();
}

} // namspace util
} // namspace vast

#endif
