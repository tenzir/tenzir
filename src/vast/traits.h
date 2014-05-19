#ifndef VAST_TRAITS_H
#define VAST_TRAITS_H

#include <memory>
#include "vast/util/intrusive.h"

namespace vast {

/// See
/// http://ericniebler.com/2013/08/07/universal-references-and-the-copy-constructo
/// for details.
template <typename A, typename B>
using disable_if_same_or_derived = std::enable_if<
  ! std::is_base_of<
    A,
    typename std::remove_reference<B>::type
  >::value
>;

template <typename A, typename B>
using DisableIfSameOrDerived = typename disable_if_same_or_derived<A, B>::type;

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

// Same as above, but takes a value as boolean expression.
template <bool If, typename Then, typename Else>
using IfThenElse = typename std::conditional<If, Then, Else>::type;

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

// SFINAE helpers
namespace detail { enum class enabler { }; }

template <typename T, typename U = void>
using EnableIf = typename std::enable_if<T::value, U>::type;

template <typename T, typename U = void>
using DisableIf = typename std::enable_if<! T::value, U>::type;

template <typename... Condition>
using EnableIfAll =
  typename std::enable_if<All<Condition...>::value, detail::enabler>::type;

template <typename... Condition>
using DisableIfAll =
  typename std::enable_if<!All<Condition...>::value, detail::enabler>::type;

template <typename T, typename U>
using EnableIfIsSame = EnableIf<std::is_same<T, U>, T>;

// Type qualifiers
template <typename T>
using RemovePointer = typename std::remove_pointer<T>::type;

/// Retrieves the unqualified type by removing const/volatile and reference
/// from a type.
template <typename T>
using Unqualified =
  typename std::remove_cv<
    typename std::remove_reference<T>::type
  >::type;

//
// Various meta functions mostly used to sfinae out certain types.
//

template <typename T>
using is_byte = Bool<sizeof(T) == 1>;

class string;

template <typename T>
using is_string =
  Bool<std::is_same<T, std::string>::value || std::is_same<T, string>::value>;

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
using is_smart_ptr =
  Bool<
    is_unique_ptr<T>::value ||
    is_shared_ptr<T>::value ||
    is_weak_ptr<T>::value ||
    is_intrusive_ptr<T>::value
  >;

template <typename T>
using is_ptr = Bool<std::is_pointer<T>::value || is_smart_ptr<T>::value>;

} // namespace vast

#endif
