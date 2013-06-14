#ifndef VAST_TRAITS_H
#define VAST_TRAITS_H

#include <memory>
#include "vast/intrusive.h"

namespace vast {

/// Meta function to determine whether a type is a byte.
template <typename T>
using is_byte = std::integral_constant<bool, sizeof(T) == 1>;

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
struct is_intrusive_ptr<intrusive_ptr<T>> : std::true_type { };


template <typename T>
using is_pointer_type = std::integral_constant<
 bool,
 std::is_pointer<T>::value ||
    is_unique_ptr<T>::value ||
    is_shared_ptr<T>::value ||
    is_weak_ptr<T>::value ||
    is_intrusive_ptr<T>::value
>;

// Integral bool
template <bool B, typename...>
using Bool = typename std::integral_constant<bool, B>::type;

// sizeof(T)
template <typename T>
using SizeOf = std::integral_constant<size_t, sizeof(T)>;

// Integral align_of
template <typename T>
using AlignOf = std::integral_constant<size_t, alignof(T)>;

// Negation
template <typename T>
using Not = Bool<!T::value>;

// If-Then-Else
template <typename If, typename Then, typename Else>
using Conditional = typename std::conditional<If::value, Then, Else>::type;

// Disjunction.
template <typename...>
struct Any : Bool<false> { };
template <typename Head, typename... Tail>
struct Any<Head, Tail...> : Conditional<Head, Bool<true>, Any<Tail...>> { };

// Conjunction
template <typename...>
struct All : Bool<true> { };
template <typename Head, typename... Tail>
struct All<Head, Tail...> : Conditional<Head, All<Tail...>, Bool<false>> { };

// A single-argument wrapper around enable_if.
namespace detail { enum class enabler { }; }
template <typename... Condition>
using EnableIf =
  typename std::enable_if<All<Condition...>::value, detail::enabler>::type;
template <typename... Condition>
using DisableIf =
  typename std::enable_if<!All<Condition...>::value, detail::enabler>::type;

template <typename T>
using Unqualified = 
  typename std::remove_cv<
    typename std::remove_reference<T>::type
  >::type;

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

} // namespace vast

#endif
