#ifndef VAST_DETAIL_TYPE_TRAITS_HPP
#define VAST_DETAIL_TYPE_TRAITS_HPP

#include <type_traits>
#include <vector>
#include <streambuf>
#include <string>
#include <tuple>

#include <caf/detail/type_traits.hpp>

namespace vast {

template <class...> class variant;

namespace detail {

using caf::detail::conjunction;
using caf::detail::disjunction;

// Computes the sum of its arguments.
template <size_t ...>
struct sum;

template <size_t S0, size_t ...SN>
struct sum<S0, SN...>
  : std::integral_constant<size_t, S0 + sum<SN...>{}> {};

template <>
struct sum<> : std::integral_constant<size_t, 0> {};

// -- is_* --------------------------------------------------------------------

template <class T>
struct is_variant : std::false_type {};

template <class... Ts>
struct is_variant<variant<Ts...>> : std::true_type {};

/// Checks whether a type is a std::tuple.
template <typename>
struct is_tuple : std::false_type {};

template <typename... Ts>
struct is_tuple<std::tuple<Ts...>> : std::true_type {};

/// Checks whether a type derives from `basic_streambuf<Char>`.
template <class T, class U = void>
struct is_streambuf : std::false_type {};

template <class T>
struct is_streambuf<
  T,
  std::enable_if_t<
    std::is_base_of<std::basic_streambuf<typename T::char_type>, T>::value
  >
> : std::true_type {};

/// Checks whether a type is container which consists of contiguous bytes.
template <class T, class U = void>
struct is_contiguous_byte_container : std::false_type {};

template <class T>
struct is_contiguous_byte_container<
  T,
  std::enable_if_t<
    std::is_same<T, std::string>::value
      || std::is_same<T, std::vector<char>>::value
      || std::is_same<T, std::vector<unsigned char>>::value
  >
> : std::true_type {};

// -- SFINAE helpers ---------------------------------------------------------
// http://bit.ly/uref-copy.

template <bool B, class T = void>
using disable_if = std::enable_if<!B, T>;

template <bool B, class T = void>
using disable_if_t = typename disable_if<B, T>::type;

template <class A, class B>
using is_same_or_derived = std::is_base_of<A, std::remove_reference_t<B>>;

template <class A, class B>
using disable_if_same_or_derived = disable_if<is_same_or_derived<A, B>::value>;

template <class A, class B>
using disable_if_same_or_derived_t =
  typename disable_if_same_or_derived<A, B>::type;

template <class T, class U, class R = T>
using enable_if_same = std::enable_if_t<std::is_same<T, U>::value, R>;

template <class T, class U, class R = T>
using disable_if_same = disable_if_t<std::is_same<T, U>::value, R>;

// -- traits -----------------------------------------------------------------

template <class T, class...Ts>
struct is_any : disjunction<std::is_same<T, Ts>::value...> {};

template <class T, class...Ts>
struct are_same : conjunction<std::is_same<T, Ts>::value...> {};

// -- tuple ------------------------------------------------------------------

// Wraps a type into a tuple if it is not already a tuple.
template <typename T>
using tuple_wrap = std::conditional_t<is_tuple<T>::value, T, std::tuple<T>>;

// Checks whether a tuple contains a given type.
template <class T, class Tuple>
struct contains;

template <class T>
struct contains<T, std::tuple<>> : std::false_type {};

template <class T, class U, class... Ts>
struct contains<T, std::tuple<U, Ts...>> : contains<T, std::tuple<Ts...>> {};

template <class T, class... Ts>
struct contains<T, std::tuple<T, Ts...>> : std::true_type {};


// -- C++17 ------------------------------------------------------------------

//template <class F, class Tuple, size_t... I>
//constexpr decltype(auto) apply_impl(F&& f, Tuple&& t,
//                                    std::index_sequence<I...> ) {
//  return invoke(std::forward<F>(f), std::get<I>(std::forward<Tuple>(t))...);
//}
//
//template <class F, class Tuple>
//constexpr decltype(auto) apply(F&& f, Tuple&& t) {
//  return apply_impl(std::forward<F>(f), std::forward<Tuple>(t),
//        std::make_index_sequence<std::tuple_size_v<std::decay_t<Tuple>>>{});
//}


} // namespace detail
} // namespace vast

#endif
