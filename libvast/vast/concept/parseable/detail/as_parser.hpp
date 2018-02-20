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

#ifndef VAST_CONCEPT_PARSEABLE_DETAIL_AS_PARSER_HPP
#define VAST_CONCEPT_PARSEABLE_DETAIL_AS_PARSER_HPP

#include <string>

#include "vast/concept/parseable/core/parser.hpp"
#include "vast/concept/parseable/core/ignore.hpp"
#include "vast/concept/parseable/string/char.hpp"
#include "vast/concept/parseable/string/string.hpp"

namespace vast {
namespace detail {

// -- unary -------------------------------------------------------------------

inline auto as_parser(char c) {
  return ignore(char_parser{c});
}

template <size_t N>
auto as_parser(char const(&str)[N]) {
  return ignore(string_parser{str});
}

inline auto as_parser(std::string str) {
  return ignore(string_parser{std::move(str)});
}

template <class T>
auto as_parser(T x)
  -> std::enable_if_t<
       std::is_arithmetic<T>{} && !std::is_same<T, bool>::value,
       decltype(ignore(string_parser{""}))
     > {
  return ignore(string_parser{std::to_string(x)});
}

template <class T>
auto as_parser(T x) -> std::enable_if_t<is_parser<T>{}, T> {
  return x; // A good compiler will elide the copy.
}

// -- binary ------------------------------------------------------------------

template <class T>
using is_convertible_to_unary_parser =
  std::integral_constant<
    bool,
    std::is_convertible<T, std::string>{}
    || (std::is_arithmetic<T>{} && !std::is_same<T, bool>::value)
  >;

template <class T, class U>
using is_convertible_to_binary_parser =
  std::integral_constant<
    bool,
    (is_parser<T>{} && is_parser<U>{})
    || (is_parser<T>{} && is_convertible_to_unary_parser<U>{})
    || (is_convertible_to_unary_parser<T>{} && is_parser<U>{})
  >;

template <
  template <class, class> class BinaryParser,
  class T,
  class U
>
using make_binary_parser =
  std::conditional_t<
    is_parser<T>{} && is_parser<U>{},
    BinaryParser<T, U>,
    std::conditional_t<
      is_parser<T>{} && is_convertible_to_unary_parser<U>{},
      BinaryParser<T, decltype(as_parser(std::declval<U>()))>,
      std::conditional_t<
        is_convertible_to_unary_parser<T>{} && is_parser<U>{},
        BinaryParser<decltype(as_parser(std::declval<T>())), U>,
        std::false_type
      >
    >
  >;

template <
  template <class, class> class BinaryParser,
  class T,
  class U
>
auto as_parser(T&& x, U&& y)
  -> std::enable_if_t<
       is_convertible_to_binary_parser<std::decay_t<T>, std::decay_t<U>>{},
       make_binary_parser<
         BinaryParser,
         decltype(detail::as_parser(std::forward<T>(x))),
         decltype(detail::as_parser(std::forward<U>(y)))
       >
     > {
  return {as_parser(std::forward<T>(x)), as_parser(std::forward<U>(y))};
}

} // namespace detail
} // namespace vast

#endif
