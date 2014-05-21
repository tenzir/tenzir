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

namespace detail {

struct can_call
{
  template <typename F, typename... A>
  static auto test(int)
    -> decltype(std::declval<F>()(std::declval<A>()...), std::true_type());

  template <typename, typename...>
  static auto test(...) -> std::false_type;
};

} // namespace detail

template<typename F, typename... A>
struct callable : decltype(detail::can_call::test<F, A...>(0)) {};

template<typename F, typename... A>
struct callable <F(A...)> : callable<F, A...> {};

template<typename... A, typename F>
constexpr callable<F, A...> is_callable_with(F&&)
{
 return callable<F(A...)>{};
}

} // namspace util
} // namspace vast

#endif
