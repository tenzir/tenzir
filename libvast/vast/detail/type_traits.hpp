/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#ifndef VAST_DETAIL_TYPE_TRAITS_HPP
#define VAST_DETAIL_TYPE_TRAITS_HPP

#include <iterator>
#include <streambuf>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include <caf/detail/type_traits.hpp>

namespace vast {

template <class...> class variant;

namespace detail {

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
template <class>
struct is_tuple : std::false_type {};

template <class... Ts>
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

template <class T>
inline constexpr bool is_contiguous_byte_container_v
  = is_contiguous_byte_container<T>::value;

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
struct is_any : std::disjunction<std::is_same<T, Ts>...> {};

template <class T, class...Ts>
struct are_same : std::conjunction<std::is_same<T, Ts>...> {};

// -- tuple ------------------------------------------------------------------

// Wraps a type into a tuple if it is not already a tuple.
template <class T>
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

template <bool B>
using bool_constant = std::integral_constant<bool, B>;

template <class...>
using void_t = void;

// -- Library Fundamentals v2 ------------------------------------------------

struct nonesuch {
  nonesuch() = delete;
  ~nonesuch() = delete;
  nonesuch(const nonesuch&) = delete;
  void operator=(const nonesuch&) = delete;
};

namespace {

template <
  class Default,
  class AlwaysVoid,
  template <class...> class Op,
  class... Args
>
struct detector {
  using value_t = std::false_type;
  using type = Default;
};

template <class Default, template<class...> class Op, class... Args>
struct detector<Default, void_t<Op<Args...>>, Op, Args...> {
  using value_t = std::true_type;
  using type = Op<Args...>;
};

} // namespace <anonymous>

template <template <class...> class Op, class... Args>
using is_detected
  = typename detector<nonesuch, void, Op, Args...>::value_t;

template <template <class...> class Op, class... Args>
using detected_t
  = typename detector<nonesuch, void, Op, Args...>::type;

template <class Default, template<class...> class Op, class... Args>
using detected_or = detector<Default, void, Op, Args...>;

template <class Default, template<class...> class Op, class... Args>
using detected_or_t = typename detected_or<Default, Op, Args...>::type;

} // namespace detail
} // namespace vast

#endif
