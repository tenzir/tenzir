#ifndef VAST_UTIL_META_H
#define VAST_UTIL_META_H

#include <memory>
#include <type_traits>

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

template <typename F, typename... A>
struct callable : decltype(detail::can_call::test<F, A...>(0)) {};

template <typename F, typename... A>
struct callable <F(A...)> : callable<F, A...> {};

template <typename... A, typename F>
constexpr callable<F, A...> is_callable_with(F&&)
{
  return callable<F(A...)>{};
}

/// @see http://bit.ly/uref-copy.
template <typename A, typename B>
using disable_if_same_or_derived = std::enable_if_t<
  ! std::is_base_of<A, std::remove_reference_t<B>>::value
>;

// Integral bool
template <bool B, typename...>
using bool_t = typename std::integral_constant<bool, B>::type;

// Negation
template <typename T>
using not_t = bool_t<! T::value>;

// Same as above, but takes a value as boolean expression.
template <typename If, typename Then, typename Else>
using if_then_else = std::conditional_t<If::value, Then, Else>;

// Disjunction.
template <typename...>
struct any : bool_t<false> {};

template <typename Head, typename... Tail>
struct any<Head, Tail...> : if_then_else<Head, bool_t<true>, any<Tail...>> { };

// Conjunction
template <typename...>
struct all : bool_t<true> { };

template <typename Head, typename... Tail>
struct all<Head, Tail...> : if_then_else<Head, all<Tail...>, bool_t<false>> { };

// SFINAE helpers
namespace detail { enum class enabler { }; }

template <bool B, typename U = void>
using disable_if = std::enable_if<! B, U>;

template <bool B, typename U = void>
using disable_if_t = typename disable_if<B, U>::type;

template <typename... Ts>
using enable_if_all = std::enable_if_t<all<Ts...>::value, detail::enabler>;

template <typename... Ts>
using disable_if_all = disable_if_t<all<Ts...>::value, detail::enabler>;

template <typename T, typename U, typename R = T>
using enable_if_same = std::enable_if_t<std::is_same<T, U>::value, R>;

template <typename T, typename U, typename R = T>
using disable_if_same = disable_if_t<std::is_same<T, U>::value, R>;

template <typename T>
using unqualified = std::remove_cv_t<std::remove_reference_t<T>>;

//
// Various meta functions mostly used to sfinae out certain types.
//

template <typename T>
using is_byte = bool_t<sizeof(T) == 1>;

template <typename T>
struct is_unique_ptr : std::false_type { };

template <typename T>
struct is_unique_ptr<std::unique_ptr<T>> : std::true_type { };

template <typename T>
struct is_shared_ptr : std::false_type { };

template <typename T>
struct is_shared_ptr<std::shared_ptr<T>> : std::true_type { };

template <typename T>
struct is_weak_ptr : std::false_type { };

template <typename T>
struct is_weak_ptr<std::weak_ptr<T>> : std::true_type { };

template <typename T>
struct is_intrusive_ptr : std::false_type { };

template <typename T>
class intrusive_ptr;

template <typename T>
struct is_intrusive_ptr<intrusive_ptr<T>> : std::true_type { };

template <typename T>
using is_smart_ptr =
  bool_t<
    is_unique_ptr<T>::value ||
    is_shared_ptr<T>::value ||
    is_weak_ptr<T>::value ||
    is_intrusive_ptr<T>::value
  >;

template <typename T>
using is_ptr = bool_t<std::is_pointer<T>::value || is_smart_ptr<T>::value>;

} // namspace util
} // namspace vast

#endif
