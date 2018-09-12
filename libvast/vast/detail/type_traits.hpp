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

#pragma once

#include <iterator>
#include <streambuf>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

#include <caf/detail/type_traits.hpp>

namespace vast::detail {

// Computes the sum of its arguments.
template <size_t ...>
struct sum;

template <size_t S0, size_t ...SN>
struct sum<S0, SN...>
  : std::integral_constant<size_t, S0 + sum<SN...>{}> {};

template <>
struct sum<> : std::integral_constant<size_t, 0> {};

// -- is_* --------------------------------------------------------------------

/// Checks whether a type is a std::tuple.
template <class>
struct is_tuple : std::false_type {};

template <class... Ts>
struct is_tuple<std::tuple<Ts...>> : std::true_type {};

template <class T>
constexpr bool is_tuple_v = is_tuple<T>::value;

/// Checks whether a type derives from `basic_streambuf<Char>`.
template <class T, class U = void>
struct is_streambuf : std::false_type {};

template <class T>
struct is_streambuf<
  T,
  std::enable_if_t<
    std::is_base_of_v<std::basic_streambuf<typename T::char_type>, T>
  >
> : std::true_type {};

template <class T>
constexpr bool is_streambuf_v = is_streambuf<T>::value;


/// Checks whether a type is container which consists of contiguous bytes.
template <class T, class U = void>
struct is_contiguous_byte_container : std::false_type {};

template <class T>
struct is_contiguous_byte_container<
  T,
  std::enable_if_t<
    std::is_same_v<T, std::string>
      || std::is_same_v<T, std::vector<char>>
      || std::is_same_v<T, std::vector<unsigned char>>
  >
> : std::true_type {};

template <class T>
constexpr bool is_contiguous_byte_container_v
  = is_contiguous_byte_container<T>::value;

// -- SFINAE helpers ---------------------------------------------------------
// http://bit.ly/uref-copy.

template <class A, class B>
constexpr bool is_same_or_derived_v
  = std::is_base_of_v<A, std::remove_reference_t<B>>;

template <bool B, class T = void>
using disable_if = std::enable_if<!B, T>;

template <bool B, class T = void>
using disable_if_t = typename disable_if<B, T>::type;

template <class A, class B>
using disable_if_same_or_derived = disable_if<is_same_or_derived_v<A, B>>;

template <class A, class B>
using disable_if_same_or_derived_t =
  typename disable_if_same_or_derived<A, B>::type;

template <class T, class U, class R = T>
using enable_if_same = std::enable_if_t<std::is_same_v<T, U>, R>;

template <class T, class U, class R = T>
using disable_if_same = disable_if_t<std::is_same_v<T, U>, R>;

// -- traits -----------------------------------------------------------------

template <class T, class... Ts>
constexpr bool is_any_v = (std::is_same_v<T, Ts> || ...);

template <class T, class... Ts>
constexpr bool are_same_v = (std::is_same_v<T, Ts> && ...);

// Utility for usage in `static_assert`. For example:
//
//   template <class T>
//   void f() {
//     if constexpr (is_same_v<T, int>)
//       ...
//     else
//       static_assert(always_false_v<T>, "error message");
//   }
//
template <class>
struct always_false : std::false_type {};

template <class T>
constexpr auto always_false_v = always_false<T>::value;

// -- tuple ------------------------------------------------------------------

// Wraps a type into a tuple if it is not already a tuple.
template <class T>
using tuple_wrap = std::conditional_t<is_tuple_v<T>, T, std::tuple<T>>;

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
constexpr bool is_detected_v
  = detector<nonesuch, void, Op, Args...>::value_t::value;

template <template <class...> class Op, class... Args>
using detected_t
  = typename detector<nonesuch, void, Op, Args...>::type;

template <class Default, template<class...> class Op, class... Args>
using detected_or = detector<Default, void, Op, Args...>;

template <class Default, template<class...> class Op, class... Args>
using detected_or_t = typename detected_or<Default, Op, Args...>::type;

// -- operator availability --------------------------------------------------

template <typename T>
using ostream_operator_t
  = decltype(std::declval<std::ostream&>() << std::declval<T>());

template <typename T>
inline constexpr bool has_ostream_operator
  = is_detected_v<ostream_operator_t, T>;

// -- checks for stringification functions -----------------------------------

template <typename T>
using to_string_t = decltype(to_string(std::declval<T>()));

template <typename T>
inline constexpr bool has_to_string = is_detected_v<to_string_t, T>;

template <typename T>
using name_getter_t =
  typename std::is_convertible<decltype(std::declval<T>().name()),
                               std::string_view>::type;

template <typename T>
inline constexpr bool has_name_getter = is_detected_v<name_getter_t, T>;

template <typename T>
using name_member_t =
  typename std::is_convertible<decltype(std::declval<T>().name),
                               std::string_view>::type;

template <typename T>
inline constexpr bool has_name_member = is_detected_v<name_member_t, T>;

} // namespace vast::detail
