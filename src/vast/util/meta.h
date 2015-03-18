#ifndef VAST_UTIL_META_H
#define VAST_UTIL_META_H

#include <memory>
#include <type_traits>
#include <caf/detail/type_list.hpp>

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

template <bool B, typename T = void>
using disable_if = std::enable_if<! B, T>;

template <bool B, typename T = void>
using disable_if_t = typename disable_if<B, T>::type;

template <typename A, typename B>
using is_same_or_derived = std::is_base_of<A, std::remove_reference_t<B>>;

/// @see http://bit.ly/uref-copy.
template <typename A, typename B>
using disable_if_same_or_derived = disable_if<is_same_or_derived<A, B>::value>;

template <typename A, typename B>
using disable_if_same_or_derived_t =
  typename disable_if_same_or_derived<A, B>::type;

template <typename... Ts>
using disable_if_all = disable_if_t<all<Ts...>::value, detail::enabler>;

template <typename T, typename U, typename R = T>
using enable_if_same = std::enable_if_t<std::is_same<T, U>::value, R>;

template <typename T, typename U, typename R = T>
using disable_if_same = disable_if_t<std::is_same<T, U>::value, R>;

template <typename T>
using unqualified = std::remove_cv_t<std::remove_reference_t<T>>;

template <typename T, typename U = std::decay_t<T>>
using deduce = std::conditional_t<
  std::is_rvalue_reference<T>::value,
  std::add_rvalue_reference_t<std::decay_t<U>>,
  std::conditional_t<
    std::is_lvalue_reference<T>::value
      && std::is_const<std::remove_reference_t<T>>::value,
    std::add_lvalue_reference_t<std::add_const_t<std::decay_t<U>>>,
    std::conditional_t<
      std::is_lvalue_reference<T>::value
        && ! std::is_const<std::remove_reference_t<T>>::value,
      std::add_lvalue_reference_t<std::decay_t<U>>,
      std::false_type
    >
  >
>;

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

//
// Type lists
//

using caf::detail::type_list;
using caf::detail::empty_type_list;
using caf::detail::is_type_list;
using caf::detail::tl_head;
using caf::detail::tl_tail;
using caf::detail::tl_size;
using caf::detail::tl_back;
using caf::detail::tl_empty;
using caf::detail::tl_slice;
using caf::detail::tl_zip;
using caf::detail::tl_unzip;
using caf::detail::tl_index_of;
using caf::detail::tl_reverse;
using caf::detail::tl_find;
using caf::detail::tl_find_if;
using caf::detail::tl_forall;
using caf::detail::tl_exists;
using caf::detail::tl_count;
using caf::detail::tl_count_not;
using caf::detail::tl_concat;
using caf::detail::tl_push_back;
using caf::detail::tl_push_front;
using caf::detail::tl_apply_all;
using caf::detail::tl_map;
using caf::detail::tl_map_conditional;
using caf::detail::tl_pop_back;
using caf::detail::tl_at;
using caf::detail::tl_prepend;
using caf::detail::tl_filter;
using caf::detail::tl_filter_not;
using caf::detail::tl_distinct;
using caf::detail::tl_is_distinct;
using caf::detail::tl_trim;
using caf::detail::tl_apply;
using caf::detail::tl_is_strict_subset;
using caf::detail::tl_equal;

} // namspace util
} // namspace vast

#endif
